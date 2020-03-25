package config

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type Config struct {
	HoldingBucket   string `json:"holdingBucket"`
	PublishedBucket string `json:"publishedBucket"`
	AggS2Level      int    `json:"aggS2Level"`
	CompareS2Level  int    `json:"compareS2Level"`
	LocalS2Level    int    `json:"localS2Level"`
}

func Get() (*Config, error) {
	resp, err := http.Get("https://storage.googleapis.com/covidtrace-config/config.json")
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := json.Unmarshal(body, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
