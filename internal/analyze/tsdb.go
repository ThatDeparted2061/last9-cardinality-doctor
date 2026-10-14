package analyze

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// TSDB reads the blocks of a Prometheus TSDB directory (a snapshot from
// /api/v1/admin/tsdb/snapshot, or a copy of a data dir) without writing to
// it and without needing a WAL. Growth is measured around Split (unix ms): a
// series is "before" if it has samples earlier than Split and "now" if it has
// samples at or after it. Chunk time ranges answer that without decoding.
type TSDB struct {
	blocks []*tsdb.Block
	Split  int64
	MinT   int64
	MaxT   int64
}

// OpenTSDB opens every block in dir; split 0 means the midpoint of the data.
func OpenTSDB(dir string, split int64) (*TSDB, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	t := &TSDB{MinT: math.MaxInt64, MaxT: math.MinInt64}
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join(dir, e.Name(), "meta.json")); !e.IsDir() || err != nil {
			continue
		}
		b, err := tsdb.OpenBlock(slog.New(slog.DiscardHandler), filepath.Join(dir, e.Name()), nil, nil)
		if err != nil {
			t.Close()
			return nil, err
		}
		t.blocks = append(t.blocks, b)
		m := b.Meta()
		t.MinT, t.MaxT = min(t.MinT, m.MinTime), max(t.MaxT, m.MaxTime)
	}
	if len(t.blocks) == 0 {
		return nil, fmt.Errorf("%s holds no TSDB blocks", dir)
	}
	t.Split = split
	if split == 0 {
		t.Split = t.MinT + (t.MaxT-t.MinT)/2
	}
	return t, nil
}

func (t *TSDB) Close() error {
	for _, b := range t.blocks {
		b.Close()
	}
	return nil
}

func (t *TSDB) chunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	var qs []storage.ChunkQuerier
	for _, b := range t.blocks {
		q, err := tsdb.NewBlockChunkQuerier(b, mint, maxt)
		if err != nil {
			return nil, err
		}
		qs = append(qs, q)
	}
	return storage.NewMergeChunkQuerier(qs, nil, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)), nil
}

// Queryable exposes the data to the PromQL engine.
func (t *TSDB) Querier(mint, maxt int64) (storage.Querier, error) {
	var qs []storage.Querier
	for _, b := range t.blocks {
		q, err := tsdb.NewBlockQuerier(b, mint, maxt)
		if err != nil {
			return nil, err
		}
		qs = append(qs, q)
	}
	return storage.NewMergeQuerier(qs, nil, storage.ChainedSeriesMerge), nil
}

func (t *TSDB) Metrics() ([]string, error) {
	q, err := t.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	names, _, err := q.LabelValues(context.Background(), labels.MetricName, nil)
	return names, err
}

func (t *TSDB) Each(metric string, fn func(Series)) error {
	q, err := t.chunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return err
	}
	defer q.Close()
	set := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric))
	var it chunks.Iterator
	for set.Next() {
		s := set.At()
		var out Series
		out.Labels = s.Labels().Map()
		it = s.Iterator(it)
		for it.Next() {
			m := it.At()
			if m.MinTime < t.Split {
				out.Before = true
			}
			if m.MaxTime >= t.Split {
				out.Now = true
			}
		}
		fn(out)
	}
	return set.Err()
}

// EachSamples streams every series of metric with its labels, for rewriting.
func (t *TSDB) EachSamples(metric string, fn func(lbls labels.Labels, ts []int64, vs []float64)) error {
	q, err := t.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return err
	}
	defer q.Close()
	set := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric))
	var ts []int64
	var vs []float64
	for set.Next() {
		s := set.At()
		ts, vs = ts[:0], vs[:0]
		it := s.Iterator(nil)
		for it.Next() != 0 {
			t, v := it.At()
			ts, vs = append(ts, t), append(vs, v)
		}
		if err := it.Err(); err != nil {
			return err
		}
		fn(s.Labels(), ts, vs)
	}
	return set.Err()
}
