package config

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type Config struct {
	ArchiveBucket   string `json:"archiveBucket"`
	HoldingBucket   string `json:"holdingBucket"`
	PublishedBucket string `json:"publishedBucket"`
	TokenBucket     string `json:"tokenBucket"`
	AggLevels       []int  `json:"aggS2Levels"`
	CompareLevel    int    `json:"compareS2Level"`
	ExposureLevel   int    `json:"exposureS2Level"`
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
