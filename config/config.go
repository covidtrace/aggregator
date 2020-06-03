package config

import (
	"encoding/json"
	"net/http"

	"github.com/covidtrace/utils/env"
)

// Config is the structure describing a configuration file
type Config struct {
	ArchiveBucket               string `json:"archiveBucket"`
	HoldingBucket               string `json:"holdingBucket"`
	PublishedBucket             string `json:"publishedBucket"`
	TokenBucket                 string `json:"tokenBucket"`
	ExposureKeysHoldingBucket   string `json:"exposureKeysHoldingBucket"`
	ExposureKeysPublishedBucket string `json:"exposureKeysPublishedBucket"`
	AggLevels                   []int  `json:"aggS2Levels"`
	CompareLevel                int    `json:"compareS2Level"`
	ExposureLevel               int    `json:"exposureS2Level"`
}

// Get fetches and unmarshals
func Get() (*Config, error) {
	resp, err := http.Get(
		env.GetDefault("CONFIG_FILE", "https://storage.googleapis.com/covidtrace-config/config.json"),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var config Config
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
