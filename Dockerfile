FROM golang:1.14

WORKDIR /go/src/github.com/covidtrace/worker
COPY . .
RUN go get -d -v ./...
RUN go install -v ./...

CMD ["serve"]