package hinting

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/worker/config"
	"github.com/golang/geo/s2"
	"google.golang.org/api/iterator"
)

var threshold int64 = 700000

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
func Run(ctx context.Context, c *config.Config, s *storage.Client) error {
	aggLevels := c.AggS2Levels
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
	var wg sync.WaitGroup
	wg.Add(len(prefixes))

	for _, prefix := range prefixes {
		go func(p string) {
			defer wg.Done()

			// Build s2 cell ID to compare level with most specific S2 level we aggregate at.
			// No use subdividing if we are looking at the most precise bucket already
			sc := s2.CellIDFromToken(p[0 : len(p)-1])
			if sc.IsValid() && sc.Level() == mostPreciseLevel {
				return
			}

			// Fetch and sum up the object sizes of everything under this geo prefix
			sizes, err := getSizes(ctx, bucket, p)
			if err != nil {
				log.Println(err)
				return
			}

			var total int64 = 0
			for _, size := range sizes {
				total += size
			}

			// Subdivide if necessary
			if total > threshold {
				key := fmt.Sprintf("%s0_HINT", p)
				ow := bucket.Object(key).NewWriter(ctx)

				_, err := io.WriteString(ow, "")
				if err != nil {
					log.Println(err)
					return
				}

				if err := ow.Close(); err != nil {
					log.Println(err)
					return
				}
			}
		}(prefix)
	}

	wg.Wait()

	return nil
}
