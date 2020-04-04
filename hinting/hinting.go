package hinting

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/worker/config"
	"github.com/golang/geo/s2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

func getIterator(ctx context.Context, bucket *storage.BucketHandle, p string, a []string) *storage.ObjectIterator {
	query := &storage.Query{Prefix: p, Delimiter: "/"}
	query.SetAttrSelection(a)
	return bucket.Objects(ctx, query)
}

func getPrefixes(ctx context.Context, bucket *storage.BucketHandle) ([]string, error) {
	var prefixes []string
	it := getIterator(ctx, bucket, "", []string{"Prefix"})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return prefixes, err
		}

		if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
		}
	}

	return prefixes, nil
}

func getSizes(ctx context.Context, bucket *storage.BucketHandle, p string) ([]int64, error) {
	var sizes []int64
	it := getIterator(ctx, bucket, p, []string{"Size"})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return sizes, err
		}

		sizes = append(sizes, attrs.Size)
	}

	return sizes, nil
}

// Run handles placing query hints in the bucket at relevant S2 geo levels
func Run(ctx context.Context, c *config.Config, s *storage.Client, throttle int64, threshold int64) error {
	aggLevels := c.AggLevels
	mostPreciseLevel := aggLevels[len(aggLevels)-1]

	bucket := s.Bucket(c.PublishedBucket)
	if bucket == nil {
		return fmt.Errorf("Bucket handle nil for: %v", c.PublishedBucket)
	}

	prefixes, err := getPrefixes(ctx, bucket)
	if err != nil {
		return err
	}

	// Wait group to fan back in afer processing
	throttler := make(chan bool, throttle)
	group, ectx := errgroup.WithContext(ctx)

	for _, prefix := range prefixes {
		throttler <- true
		group.Go(func() error {
			defer func() { <-throttler }()

			// Build s2 cell ID to compare level with most specific S2 level we aggregate at.
			// No use subdividing if we are looking at the most precise bucket already
			sc := s2.CellIDFromToken(prefix[0 : len(prefix)-1])
			if sc.IsValid() && sc.Level() == mostPreciseLevel {
				return nil
			}

			// Fetch and sum up the object sizes of everything under this geo prefix
			sizes, err := getSizes(ectx, bucket, prefix)
			if err != nil {
				return err
			}

			var total int64 = 0
			for _, size := range sizes {
				total += size
			}

			// Subdivide if necessary
			if total > threshold {
				key := fmt.Sprintf("%s0_HINT", prefix)
				ow := bucket.Object(key).NewWriter(ectx)

				if _, err := io.WriteString(ow, ""); err != nil {
					return err
				}

				return ow.Close()
			}

			return nil
		})
	}

	return group.Wait()
}
