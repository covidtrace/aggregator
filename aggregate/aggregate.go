package aggregate

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/aggregator/config"
	"github.com/golang/geo/s2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

type objects []string
type record []string
type records []record

type point struct {
	timestamp time.Time
	cellID    s2.CellID
	verified  bool
}

type pointBuckets map[s2.CellID][]point

type token struct {
	timestamp time.Time
	uuid      string
	cellID    s2.CellID
}

type tokenBuckets map[s2.CellID][]token

func getObjectReaders(ctx context.Context, bucket *storage.BucketHandle) ([]io.ReadCloser, objects, error) {
	rds := []io.ReadCloser{}
	obs := objects{}

	q := &storage.Query{Prefix: ""}
	q.SetAttrSelection([]string{"Name"})

	it := bucket.Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return rds, obs, err
		}

		if strings.HasSuffix(attrs.Name, ".csv") {
			obs = append(obs, attrs.Name)
		}
	}

	for _, n := range obs {
		o := bucket.Object(n)
		or, err := o.NewReader(ctx)
		if err != nil {
			return rds, obs, err
		}

		rds = append(rds, or)
	}

	return rds, obs, nil
}

func csvReader(r io.Reader, fpr int) *csv.Reader {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = fpr
	cr.ReuseRecord = false
	cr.TrimLeadingSpace = true
	return cr
}

func getRecords(rds []io.ReadCloser, sh bool, fpr int) (records, error) {
	rs := records{}

	for _, r := range rds {
		cr := csvReader(r, fpr)

		// Skip header row if instructed
		if sh {
			if _, err := cr.Read(); err == io.EOF {
				return rs, nil
			} else if err != nil {
				return rs, err
			}
		}

		// Read remaining rows
		for {
			if r, err := cr.Read(); err == io.EOF {
				break
			} else if err != nil {
				continue
			} else if len(r) == fpr {
				rs = append(rs, r)
			}
		}

		r.Close()
	}

	return rs, nil
}

func recordsToPoints(rs records) []point {
	points := []point{}

	for _, r := range rs {
		if ts, err := strconv.ParseInt(r[0], 10, 64); err == nil {
			cellID := s2.CellIDFromToken(r[1])
			v := r[2] == "true"

			if cellID.IsValid() {
				points = append(points, point{
					timestamp: time.Unix(ts, 0),
					cellID:    cellID,
					verified:  v,
				})
			}
		}
	}

	return points
}

func recordsToTokens(rs records) []token {
	tokens := []token{}

	for _, r := range rs {
		if ts, err := strconv.ParseInt(r[0], 10, 64); err == nil {
			uuid := r[1]
			cellID := s2.CellIDFromToken(r[2])

			if cellID.IsValid() {
				tokens = append(tokens, token{
					timestamp: time.Unix(ts, 0),
					uuid:      uuid,
					cellID:    cellID,
				})
			}
		}
	}

	return tokens
}

func bucketPoints(c *config.Config, points []point) pointBuckets {
	bucket := make(pointBuckets)

	for _, p := range points {
		for _, aggLevel := range c.AggLevels {
			bcid := p.cellID.Parent(aggLevel)
			bucket[bcid] = append(bucket[bcid], p)
		}
	}

	return bucket
}

func bucketTokens(c *config.Config, tokens []token) tokenBuckets {
	buckets := make(tokenBuckets)

	for _, t := range tokens {
		bcid := t.cellID.Parent(c.ExposureLevel)
		buckets[bcid] = append(buckets[bcid], t)
	}

	return buckets
}

func writeRecords(ctx context.Context, object *storage.ObjectHandle, rs records) error {
	wc := object.NewWriter(ctx)
	w := csv.NewWriter(wc)
	for _, r := range rs {
		if err := w.Write(r); err != nil {
			return err
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}

	return wc.Close()
}

func pointsToRecords(c *config.Config, points []point) records {
	rs := make(records, len(points))
	for i, p := range points {
		rs[i] = record{
			fmt.Sprintf("%v", p.timestamp.Unix()),
			p.cellID.Parent(c.CompareLevel).ToToken(),
			fmt.Sprintf("%v", p.verified),
		}
	}
	return rs
}

func tokensToRecords(c *config.Config, tokens []token) records {
	rs := make(records, len(tokens))
	for i, t := range tokens {
		rs[i] = record{fmt.Sprintf("%v", t.timestamp.Unix()), t.uuid}
	}
	return rs
}

func archiveObjects(ctx context.Context, src, dst *storage.BucketHandle, pre string, obs objects, sem chan bool) error {
	group, ectx := errgroup.WithContext(ctx)

	for _, name := range obs {
		sem <- true

		group.Go(func() error {
			defer func() { <-sem }()

			so := src.Object(name)
			do := dst.Object(fmt.Sprintf("%s/%s", pre, name))

			if _, err := do.CopierFrom(so).Run(ectx); err != nil {
				return err
			}

			if err := so.Delete(ectx); err != nil {
				return err
			}

			return nil
		})
	}

	return group.Wait()
}

func csvObjectName(cid s2.CellID, ts time.Time, t string) string {
	return fmt.Sprintf("%s/%v.%s.csv", cid.ToToken(), ts.Unix(), t)
}

// Holding handles aggregating all the input points in a holding bucket and publishing to
// a publish bucket
func Holding(ctx context.Context, c *config.Config, s *storage.Client, throttle int64) error {
	readers, objects, err := getObjectReaders(ctx, s.Bucket(c.HoldingBucket))
	if err != nil {
		return err
	}

	records, err := getRecords(readers, true, 3)
	if err != nil {
		return err
	}

	points := recordsToPoints(records)

	// Control goroutine concurrency
	sem := make(chan bool, throttle)

	if len(points) != 0 {
		buckets := bucketPoints(c, points)

		// Create a wait group to fan back in after uploading
		group, ectx := errgroup.WithContext(ctx)

		for bucket, points := range buckets {
			sem <- true

			group.Go(func() error {
				defer func() { <-sem }()
				o := s.Bucket(c.PublishedBucket).Object(csvObjectName(bucket, time.Now(), "points"))
				return writeRecords(ectx, o, pointsToRecords(c, points))
			})
		}

		if err := group.Wait(); err != nil {
			return err
		}
	}

	return archiveObjects(
		ctx,
		s.Bucket(c.HoldingBucket),
		s.Bucket(c.ArchiveBucket),
		"holding",
		objects,
		sem,
	)
}

// Tokens handles aggregating all the input tokens in a holding bucket and publishing to
// a publish bucket
func Tokens(ctx context.Context, c *config.Config, s *storage.Client, throttle int64) error {
	readers, objects, err := getObjectReaders(ctx, s.Bucket(c.TokenBucket))
	if err != nil {
		return err
	}

	records, err := getRecords(readers, true, 3)
	if err != nil {
		return err
	}

	tokens := recordsToTokens(records)

	// Control goroutine concurrency
	sem := make(chan bool, throttle)

	if len(tokens) != 0 {
		buckets := bucketTokens(c, tokens)

		// Create a wait group to fan back in after uploading
		group, ectx := errgroup.WithContext(ctx)

		for bucket, tokens := range buckets {
			sem <- true

			group.Go(func() error {
				defer func() { <-sem }()
				o := s.Bucket(c.PublishedBucket).Object(csvObjectName(bucket, time.Now(), "tokens"))
				return writeRecords(ectx, o, tokensToRecords(c, tokens))
			})
		}

		if err := group.Wait(); err != nil {
			return err
		}
	}

	return archiveObjects(
		ctx,
		s.Bucket(c.TokenBucket),
		s.Bucket(c.ArchiveBucket),
		"tokens",
		objects,
		sem,
	)
}
