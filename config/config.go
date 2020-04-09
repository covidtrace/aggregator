package config

import (
	"encoding/json"
	"net/http"
	"os"
)

var configFile string

func init() {
	configFile = os.Getenv("CONFIG_FILE")
	if configFile == "" {
		configFile = "https://storage.googleapis.com/covidtrace-config/config.json"
	}
}

// Config is the structure describing a configuration file
type Config struct {
	ArchiveBucket   string `json:"archiveBucket"`
	HoldingBucket   string `json:"holdingBucket"`
	PublishedBucket string `json:"publishedBucket"`
	TokenBucket     string `json:"tokenBucket"`
	AggLevels       []int  `json:"aggS2Levels"`
	CompareLevel    int    `json:"compareS2Level"`
	ExposureLevel   int    `json:"exposureS2Level"`
}

// Get fetches and unmarshals
func Get() (*Config, error) {
	resp, err := http.Get(configFile)
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
