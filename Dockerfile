FROM golang:1.14

WORKDIR /go/src/github.com/covidtrace/aggregator
COPY . .
RUN go get -d -v ./...
RUN go install -v ./...

CMD ["serve"]
