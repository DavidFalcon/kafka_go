package connector

import (
	"context"
	"fmt"
	"os"
	"time"
    "strings"
    "sort"
    "sync"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"../utils"
)

// CreateTopic creates a topic using the Admin Client API
func CreateTopic(p *kafka.Producer, topic string) {

	a, err := kafka.NewAdminClientFromProducer(p)
	if err != nil {
		fmt.Printf("Failed to create new admin client from producer: %s", err)
		os.Exit(1)
	}
	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create topics on cluster.
	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		fmt.Printf("ParseDuration(60s): %s", err)
		os.Exit(1)
	}
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Admin Client request error: %v\n", err)
		os.Exit(1)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Printf("Failed to create topic: %v\n", result.Error)
			os.Exit(1)
		}
		fmt.Printf("%v\n", result)
	}
	a.Close()

}

// CreateProducer creates a producer
func CreateProducer(conf map[string]string) *kafka.Producer {
	// Create Producer instance
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": conf["bootstrap.servers"],
		"broker.address.family": "v4",
		"go.delivery.reports" : false})
	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	return p
}

// GetProducer creates a producer and a topic if need
func GetProducer(conf map[string]string, topic *string) *kafka.Producer {
    // Create Producer instance
    producer := CreateProducer(conf)

    // Create topic if needed
    CreateTopic(producer, *topic)

    return producer
}

// CreateProducer creates a consumer
func CreateConsumer(conf map[string]string) *kafka.Consumer {
	// Create Consumer instance
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": conf["bootstrap.servers"],
		"group.id":          "go_group_1",
		"auto.offset.reset": "earliest"})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	return c
}

// Push date to topic 'topic' to any partition
func Push(producer *kafka.Producer, topic *string, recordValue string) {
        producer.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
            Value:          []byte(recordValue),
        }, nil)
}

// Pull date from Kafka and fill arrays
func PullMessages(consumer *kafka.Consumer,
                  inputBuffer     []string,
                  idBuffer        []utils.SortedIntRef,
                  nameBuffer      []utils.SortedStringRef,
                  addressBuffer   []utils.SortedStringRef,
                  continentBuffer []utils.SortedStringRef,
                  sigchan         chan os.Signal,
                  conf map[string]string) {
	current := 0
	run := true
	for run == true && current < len(inputBuffer) {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(time.Duration(utils.GetInt(conf["consumer.read.time"])) * time.Millisecond)
			if err != nil {
                // The client will automatically try to recover from all errors.
                fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				continue
			}
			inputBuffer[current] = string(msg.Value)
			rowDate := strings.Split(inputBuffer[current], ",")

			idBuffer[current]        = utils.SortedIntRef{utils.GetInt32(rowDate[0]), &inputBuffer[current]}
			nameBuffer[current]      = utils.SortedStringRef{rowDate[1], &inputBuffer[current]}
            addressBuffer[current]   = utils.SortedStringRef{rowDate[2], &inputBuffer[current]}
            continentBuffer[current] = utils.SortedStringRef{rowDate[3], &inputBuffer[current]}
            fmt.Printf("Consumed record value %s, and updated total count to %d\n", inputBuffer[current], current)
			current++
		}
	}
}

// Processing records that have type Int
func ProcessIntField(conf map[string]string, date []utils.SortedIntRef, topic string) {
    sort.SliceStable(date, func(i, j int) bool {
        return date[i].IntField < date[j].IntField
    })
    // Get Producer instance
    producer:= GetProducer(conf, &topic)

    for _, element := range date {
        Push(producer, &topic, *element.RawDate)
    }

    // Wait for all messages to be delivered
    producer.Flush(utils.GetInt(conf["producer.wait.time"]))
}

// Processing records that have type String
func ProcessStringField(wg *sync.WaitGroup, conf map[string]string, date []utils.SortedStringRef, topic string) {
    defer wg.Done()

    sort.SliceStable(date, func(i, j int) bool {
        return date[i].StrField < date[j].StrField
    })

    // Get Producer instance
    producer:= GetProducer(conf, &topic)

    for _, element := range date {
        Push(producer, &topic, *element.RawDate)
    }

    // Wait for all messages to be delivered
    producer.Flush(utils.GetInt(conf["producer.wait.time"]))
}
