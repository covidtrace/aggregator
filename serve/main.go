package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/aggregator/aggregate"
	"github.com/covidtrace/aggregator/config"
	"github.com/covidtrace/aggregator/hinting"
	"github.com/covidtrace/utils/env"
	httputils "github.com/covidtrace/utils/http"
	"github.com/julienschmidt/httprouter"
	"golang.org/x/sync/errgroup"
)

type response struct {
	Success bool `json:"success"`
}

func main() {
	storageClient, err := storage.NewClient(context.Background())
	if err != nil {
		panic(err)
	}

	// split at 2MiB by default
	threshold, err := strconv.ParseInt(env.GetDefault("HINTING_THRESHOLD", "2097152"), 0, 64)
	if err != nil {
		panic(err)
	}

	// 10 goroutines max by default
	goroutineLimit, err := strconv.ParseInt(env.GetDefault("GOROUTINE_LIMIT", "10"), 0, 64)
	if err != nil {
		panic(err)
	}

	router := httprouter.New()

	router.POST("/aggregate", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		group, ctx := errgroup.WithContext(context.Background())

		config, err := config.Get()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			httputils.ReplyInternalServerError(w, err)
			return
		}

		group.Go(func() error {
			return aggregate.Holding(ctx, config, storageClient, goroutineLimit)
		})

		group.Go(func() error {
			return aggregate.Tokens(ctx, config, storageClient, goroutineLimit)
		})

		group.Go(func() error {
			return aggregate.ExposureKeys(ctx, config, storageClient, goroutineLimit)
		})

		if err := group.Wait(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			httputils.ReplyInternalServerError(w, err)
			return
		}

		httputils.ReplyJSON(w, response{Success: true}, http.StatusOK)
	})

	router.POST("/hinting", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		ctx := context.Background()

		config, err := config.Get()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			httputils.ReplyInternalServerError(w, err)
			return
		}

		if err := hinting.Run(ctx, config, storageClient, goroutineLimit, threshold); err != nil {
			fmt.Fprintln(os.Stderr, err)
			httputils.ReplyInternalServerError(w, err)
			return
		}

		httputils.ReplyJSON(w, response{Success: true}, http.StatusOK)
	})

	router.PanicHandler = func(w http.ResponseWriter, _ *http.Request, _ interface{}) {
		httputils.ReplyInternalServerError(w, errors.New("internal server error"))
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", env.GetDefault("port", "8080")), router))
}
