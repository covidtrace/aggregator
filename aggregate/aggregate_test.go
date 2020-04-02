package aggregate

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/worker/config"
)

func TestRun(t *testing.T) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	config, err := config.Get()
	if err != nil {
		t.Fatal(err)
	}

	err = Run(context.Background(), config, client, 1)
	if err != nil {
		t.Fatal(err)
	}
}
