package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/covidtrace/worker/aggregate"
	"github.com/covidtrace/worker/config"
	"github.com/covidtrace/worker/hinting"
	"github.com/julienschmidt/httprouter"
	"golang.org/x/sync/errgroup"
)

var storageClient *storage.Client

var threshold int64 = 2097152 // split at 2Mbi by default
var goroutineLimit int64 = 10

func init() {
	c, err := storage.NewClient(context.Background())
	if err != nil {
		panic(err)
	}

	storageClient = c

	if t := os.Getenv("HINTING_THRESHOLD"); t != "" {
		var err error
		threshold, err = strconv.ParseInt(t, 0, 64)
		if err != nil {
			panic(err)
		}
	}

	if gl := os.Getenv("GOROUTINE_LIMIT"); gl != "" {
		var err error
		goroutineLimit, err = strconv.ParseInt(gl, 0, 64)
		if err != nil {
			panic(err)
		}
	}
}

type response struct {
	Success bool `json:"success"`
}

type errorResponse struct {
	Message string `json:"message"`
}

func replyJSON(w http.ResponseWriter, code int, r interface{}) {
	b, err := json.Marshal(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(code)
	io.Copy(w, bytes.NewReader(b))
}

func main() {
	router := httprouter.New()

	router.POST("/aggregate", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		group, ctx := errgroup.WithContext(context.Background())

		config, err := config.Get()
		if err != nil {
			panic(err)
		}

		group.Go(func() error {
			return aggregate.Holding(ctx, config, storageClient, goroutineLimit)
		})

		group.Go(func() error {
			return aggregate.Tokens(ctx, config, storageClient, goroutineLimit)
		})

		if err := group.Wait(); err != nil {
			replyJSON(w, http.StatusInternalServerError, errorResponse{Message: err.Error()})
			return
		}

		replyJSON(w, http.StatusOK, response{
			Success: true,
		})
	})

	router.POST("/hinting", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		ctx := context.Background()

		config, err := config.Get()
		if err != nil {
			panic(err)
		}

		if err := hinting.Run(ctx, config, storageClient, goroutineLimit, threshold); err != nil {
			panic(err)
		}

		replyJSON(w, http.StatusOK, response{
			Success: true,
		})
	})

	router.PanicHandler = func(w http.ResponseWriter, _ *http.Request, _ interface{}) {
		http.Error(w, "Unknown error", http.StatusBadRequest)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), router))
}
