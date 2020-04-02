package aggregate

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/worker/config"
	"github.com/golang/geo/s2"
	"google.golang.org/api/iterator"
)

type point struct {
	timestamp time.Time
	cellID    s2.CellID
	verified  bool
}

type pointBuckets map[s2.CellID][]point

func parseCsv(reader io.Reader) ([]point, error) {
	points := []point{}

	records := csv.NewReader(reader)

	records.FieldsPerRecord = 3
	records.ReuseRecord = true
	records.TrimLeadingSpace = true

	_, err := records.Read()
	if err == io.EOF {
		return points, nil
	}

	if err != nil {
		return points, err
	}

	for {
		record, err := records.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			continue
		}

		if len(record) == 3 {
			if timestamp, err := strconv.ParseInt(record[0], 10, 64); err != nil {
				cellID := s2.CellIDFromToken(record[1])
				verified := record[2] == "true"

				if cellID.IsValid() {
					points = append(points, point{
						timestamp: time.Unix(timestamp, 0),
						cellID:    cellID,
						verified:  verified,
					})
				}
			}
		}
	}

	return points, nil
}

func getHoldingFiles(ctx context.Context, client *storage.Client, bucket string) ([]point, []string, error) {
	points := []point{}

	inbucket := client.Bucket(bucket)

	query := &storage.Query{Prefix: ""}
	query.SetAttrSelection([]string{"Name"})

	objects := inbucket.Objects(ctx, query)
	names := []string{}

	for {
		attrs, err := objects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return points, names, err
		}

		if strings.HasSuffix(attrs.Name, ".csv") {
			names = append(names, attrs.Name)
		}
	}

	for _, name := range names {
		object := inbucket.Object(name)
		reader, err := object.NewReader(ctx)
		if err != nil {
			return points, names, err
		}
		defer reader.Close()

		p, err := parseCsv(reader)
		if err != nil {
			return points, names, err
		}

		points = append(points, p...)
	}

	return points, names, nil
}

func bucketPoints(c *config.Config, points []point) (pointBuckets, error) {
	bucket := make(pointBuckets)

	for _, p := range points {
		for _, aggLevel := range c.AggS2Levels {
			bcid := p.cellID.Parent(aggLevel)
			bucket[bcid] = append(bucket[bcid], p)
		}
	}

	return bucket, nil
}

func writePoints(ctx context.Context, c *config.Config, object *storage.ObjectHandle, points []point) error {
	wc := object.NewWriter(ctx)
	writer := csv.NewWriter(wc)
	for _, point := range points {
		err := writer.Write([]string{
			fmt.Sprintf("%v", point.timestamp.Unix()),
			point.cellID.Parent(c.CompareS2Level).ToToken(),
			fmt.Sprintf("%v", point.verified),
		})
		if err != nil {
			return err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	return wc.Close()
}

// Run handles aggregating all the input points in a holding bucket and publishing to
// a publish bucket
func Run(ctx context.Context, c *config.Config, s *storage.Client, throttle int64) error {
	points, objects, err := getHoldingFiles(ctx, s, c.HoldingBucket)
	if err != nil {
		return err
	}

	// Control goroutine concurrency
	throttler := make(chan bool, throttle)

	if len(points) != 0 {
		buckets, err := bucketPoints(c, points)
		if err != nil {
			return err
		}

		// Create a wait group to fan back in after uploading
		var wg1 sync.WaitGroup
		wg1.Add(len(buckets))

		for bucket, points := range buckets {
			throttler <- true
			go func(b s2.CellID, p []point) {
				defer func() { <-throttler }()
				defer wg1.Done()

				o := s.Bucket(c.PublishedBucket).Object(
					fmt.Sprintf("%v/%v.csv", b.ToToken(), time.Now().Unix()),
				)

				if err = writePoints(ctx, c, o, p); err != nil {
					log.Println(err)
					return
				}
			}(bucket, points)
		}

		wg1.Wait()
	}

	sb := s.Bucket(c.HoldingBucket)
	db := s.Bucket(c.ArchiveBucket)

	var wg2 sync.WaitGroup
	wg2.Add(len(objects))

	for _, name := range objects {
		throttler <- true
		go func(n string) {
			defer func() { <-throttler }()
			defer wg2.Done()

			src := sb.Object(n)
			dst := db.Object(fmt.Sprintf("holding/%s", n))

			if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
				log.Println(err)
				return
			}

			if err := src.Delete(ctx); err != nil {
				log.Println(err)
				return
			}
		}(name)
	}

	wg2.Wait()

	return nil
}
