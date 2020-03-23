package aggregate

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
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

func bucketPoints(points []point, level int) (pointBuckets, error) {
	bucket := make(pointBuckets)

	for _, p := range points {
		bcid := p.cellID.Parent(level)
		bucket[bcid] = append(bucket[bcid], p)
	}

	return bucket, nil
}

func Bucket(ctx context.Context, in, out string) error {
	inbucket := client.Bucket(in)

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
			return err
		}

		names = append(names, attrs.Name)
	}

	if len(names) == 0 {
		return nil
	}

	points := []point{}
	for _, name := range names {
		object := inbucket.Object(name)
		reader, err := object.NewReader(ctx)
		if err != nil {
			return err
		}
		defer reader.Close()

		p, err := parseCsv(reader)
		if err != nil {
			return err
		}

		points = append(points, p...)
	}

	buckets, err := bucketPoints(points, 5)
	if err != nil {
		return err
	}

	for bucket, points := range buckets {
		object := client.Bucket(out).Object(
			fmt.Sprintf("%v/%v.csv", bucket.ToToken(), time.Now().Unix()),
		)

		wc := object.NewWriter(ctx)
		writer := csv.NewWriter(wc)
		for _, point := range points {
			err = writer.Write([]string{point.timestamp, point.cellID.ToToken(), point.status})
			if err != nil {
				return err
			}
		}

		writer.Flush()
		if err := writer.Error(); err != nil {
			return err
		}

		if err := wc.Close(); err != nil {
			return err
		}
	}

	for _, name := range names {
		object := inbucket.Object(name)
		err = object.Delete(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
