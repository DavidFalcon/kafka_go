FROM golang:1.16

ENV GO111MODULE=off
ENV GOPATH /go

RUN apt update && apt install bash ca-certificates git gcc g++ libc-dev librdkafka-dev pkgconf make -y

WORKDIR /build
RUN git clone https://github.com/edenhill/librdkafka.git
WORKDIR /build/librdkafka
RUN /build/librdkafka/configure --prefix /usr
RUN make
RUN make install

RUN go get gopkg.in/confluentinc/confluent-kafka-go.v1/kafka

WORKDIR /go/src/app/
COPY . .

WORKDIR /go/src/app/producer
RUN go build producer.go

WORKDIR /go/src/app/consumer
RUN go build consumer.go
