FROM golang:alpine

RUN set -ex &&\
    apk add --no-progress --no-cache \
      gcc \
      musl-dev

RUN mkdir /app
WORKDIR /app

RUN go get gopkg.in/confluentinc/confluent-kafka-go.v1/kafka

COPY . .

RUN go build producer.go
RUN go build consumer.go
