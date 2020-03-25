package aggregate

import (
	"context"
	"testing"

	"github.com/covidtrace/worker/config"
)

func TestHandleEvent(t *testing.T) {
	config, err := config.Get()
	if err != nil {
		t.Fatal(err)
	}

	err = Bucket(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
}
