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

	"github.com/covidtrace/worker/aggregate"
	"github.com/covidtrace/worker/config"
	"github.com/julienschmidt/httprouter"
)

func main() {
	router := httprouter.New()

	router.POST("/", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		ctx := context.Background()

		config, err := config.Get()
		if err != nil {
			panic(err)
		}

		if err := aggregate.Bucket(ctx, config); err != nil {
			panic(err)
		}

		m := struct {
			Status string `json:"status"`
		}{
			Status: "success",
		}

		b, err := json.Marshal(m)
		if err != nil {
			panic(err)
		}

		io.Copy(w, bytes.NewReader(b))

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
