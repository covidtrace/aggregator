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
	"github.com/julienschmidt/httprouter"
)

func handler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ctx := context.Background()

	if err := aggregate.Bucket(ctx, "covidtrace-holding", "covidtrace-published"); err != nil {
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
}

func main() {
	router := httprouter.New()

	router.POST("/", handler)

	router.PanicHandler = func(w http.ResponseWriter, _ *http.Request, _ interface{}) {
		http.Error(w, "Unknown error", http.StatusBadRequest)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), router))
}
