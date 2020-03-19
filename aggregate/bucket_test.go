package aggregate

import (
	"context"
	"testing"
)

func TestHandleEvent(t *testing.T) {
	err := Bucket(context.Background(), "covidtrace-holding", "covidtrace-published")
	if err != nil {
		t.Fatal(err)
	}
}
