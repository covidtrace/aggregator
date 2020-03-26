package aggregate

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/worker/config"
	"github.com/golang/geo/s2"
	"google.golang.org/api/iterator"
)

var client *storage.Client

func init() {
	c, err := storage.NewClient(context.Background())
	if err != nil {
		panic(err)
	}

	client = c
}

type point struct {
	timestamp string
	cellID    s2.CellID
	status    string
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

		timestamp := record[0]
		cellID := s2.CellIDFromToken(record[1])
		status := record[2]

		points = append(points, point{timestamp: timestamp, cellID: cellID, status: status})
	}

	return points, nil
}

func getHoldingFiles(ctx context.Context, bucket string) ([]point, error) {
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
			return points, err
		}

		names = append(names, attrs.Name)
	}

	for _, name := range names {
		object := inbucket.Object(name)
		reader, err := object.NewReader(ctx)
		if err != nil {
			return points, err
		}
		defer reader.Close()

		p, err := parseCsv(reader)
		if err != nil {
			return points, err
		}

		points = append(points, p...)
	}

	return points, nil
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
			point.timestamp,
			point.cellID.Parent(c.CompareS2Level).ToToken(),
			point.cellID.Parent(c.LocalS2Level).ToToken(),
			point.status,
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

func Run(ctx context.Context, c *config.Config) error {
	points, err := getHoldingFiles(ctx, c.HoldingBucket)
	if err != nil {
		return err
	}

	if len(points) == 0 {
		return nil
	}

	buckets, err := bucketPoints(c, points)
	if err != nil {
		return err
	}

	for bucket, points := range buckets {
		object := client.Bucket(c.PublishedBucket).Object(
			fmt.Sprintf("%v/%v.csv", bucket.ToToken(), time.Now().Unix()),
		)

		if err = writePoints(ctx, c, object, points); err != nil {
			return err
		}
	}

	return nil
}
