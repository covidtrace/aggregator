#!/usr/bin/env bash

set -xeuo pipefail

docker build -t gcr.io/covidtrace/aggregator .
docker push gcr.io/covidtrace/aggregator