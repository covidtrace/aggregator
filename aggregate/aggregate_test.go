package aggregate

import (
	"context"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/aggregator/config"
	"github.com/golang/geo/s2"
)

var c *config.Config

func init() {
	c = &config.Config{
		ArchiveBucket:   "covidtrace-archive",
		HoldingBucket:   "covidtrace-holding",
		PublishedBucket: "covidtrace-published",
		TokenBucket:     "covidtrace-tokens",
		AggLevels:       []int{8, 10, 12},
		CompareLevel:    18,
		ExposureLevel:   10,
	}
}

func TestHolding(t *testing.T) {
	ctx := context.Background()
	s, err := storage.NewClient(ctx)
	if err != nil {
		t.Error(err)
	}

	if err := Holding(ctx, c, s, 1); err != nil {
		t.Error(err)
	}
}

func TestGetRecords(t *testing.T) {
	readers := []io.ReadCloser{
		ioutil.NopCloser(strings.NewReader("foo,bar,baz\r\n1,2,3\r\n4,5,6")),
	}

	records, err := getRecords(readers, true, 3)
	if err != nil {
		t.Errorf("getRecords: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("Unexpected len(records): %v", len(records))
	}

	if records[0][0] != "1" {
		t.Errorf("Unexpected records[0][0]: %v", records[0][0])
	}

	if records[1][0] != "4" {
		t.Errorf("Unexpected records[1][0]: %v", records[1][0])
	}

	readers = []io.ReadCloser{
		ioutil.NopCloser(strings.NewReader("foo,bar,baz\r\n1,2,3\r\n4,5,6")),
	}

	records, err = getRecords(readers, false, 3)
	if err != nil {
		t.Errorf("getRecords: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("Unexpected len(records): %v", len(records))
	}
}

func TestRecordsToPoints(t *testing.T) {
	rs := records{record{"1586042466", "54906ad", "false"}}
	points := recordsToPoints(rs)

	if len(points) != 1 {
		t.Errorf("Unexpected len(points): %v", len(points))
	}

	point := points[0]

	if point.timestamp.Unix() != 1586042466 {
		t.Errorf("Unexpected point.timestamp: %v", point.timestamp.Unix())
	}

	if !point.cellID.IsValid() {
		t.Error("Expected point.cellID.IsValid() to be true")
	}

	if point.verified {
		t.Error("Expected point.verified to be false")
	}
}

func TestRecordsToTokens(t *testing.T) {
	rs := records{record{"1586291781", "uuid", "54906ad"}}
	tokens := recordsToTokens(rs)

	if len(tokens) != 1 {
		t.Errorf("Unexpected len(tokens): %v", len(tokens))
	}

	token := tokens[0]

	if token.timestamp.Unix() != 1586291781 {
		t.Errorf("Unexpected token.timestamp: %v", token.timestamp.Unix())
	}

	if token.uuid != "uuid" {
		t.Errorf("Unexpected token.uuid: %v", token.uuid)
	}

	if !token.cellID.IsValid() {
		t.Error("Expected token.cellID.IsValid() to be true")
	}
}

func TestBucketPoints(t *testing.T) {
	points := []point{
		{time.Now(), s2.CellIDFromToken("808fb5b0be4b"), false},
		{time.Now(), s2.CellIDFromToken("5490153531d3"), false},
	}

	buckets := bucketPoints(c, points)
	if len(buckets) != 2*len(c.AggLevels) {
		t.Errorf("Unexpected len(buckets): %v", len(buckets))
	}

	if _, ok := buckets[s2.CellIDFromToken("808fb5b")]; !ok {
		t.Errorf("Expected to find 808fb5b in buckets")
	}

	if _, ok := buckets[s2.CellIDFromToken("5490153")]; !ok {
		t.Errorf("Expected to find 5490153 in buckets")
	}
}

func TestBucketTokens(t *testing.T) {
	tokens := []token{
		{time.Now(), "uuid1", s2.CellIDFromToken("808fb5b0be4b")},
		{time.Now(), "uuid2", s2.CellIDFromToken("5490153531d3")},
	}

	buckets := bucketTokens(c, tokens)
	if len(buckets) != 2 {
		t.Errorf("Unexpected len(buckets): %v", len(buckets))
	}

	if _, ok := buckets[s2.CellIDFromToken("808fb5")]; !ok {
		t.Errorf("Expected to find 808fb5 in buckets")
	}

	if _, ok := buckets[s2.CellIDFromToken("549015")]; !ok {
		t.Errorf("Expected to find 549015 in buckets")
	}
}
