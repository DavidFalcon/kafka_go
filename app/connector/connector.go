package connector

import (
    "os"
    "fmt"
    "time"
    "context"
    "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
    "utils"
    "rand"
)

type SortFields func([]string)

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

// Push date to topic 'topic' to 'partition'
func Push(producer *kafka.Producer, topic *string, recordValue string) {
    producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
        Value:          []byte(recordValue),
    }, nil)
}

// Generate and Push date to topic 'topic' to any partition
func PushMessages(conf map[string]string,
                  topic *string) {

    // Get Producer instance
    producer:= GetProducer(conf, topic)

    for n := 0; n < utils.GetInt(conf["date.count"]); n++ {
      fmt.Printf("Push #%d\n", n)
      Push(producer, topic, rand.RandRecord())
    }
    // Wait for all messages to be delivered
    producer.Flush(utils.GetInt(conf["producer.wait.time"]))
    producer.Close()
}


// Pull date from Kafka and fill arrays
func PullMessages(topic *string,
                  parsedBuffer    []string,
                  sigchan         chan os.Signal,
                  conf map[string]string){

    // Create Consumer instance
    consumer:= CreateConsumer(conf)

    // Subscribe to topic
    consumer.SubscribeTopics([]string{*topic}, nil)

    current := 0
    run := true
    for run == true && current < utils.GetInt(conf["date.count"]) {
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
    		parsedBuffer[current] = string(msg.Value)
            fmt.Printf("Consumed record value %s, and updated total count to %d\n", parsedBuffer[current], current)
    		current++
    	}
    }
    fmt.Printf("Closing consumer\n")
    consumer.Close()
}

// Processing records
func ProcessFields(producer *kafka.Producer,
                   conf map[string]string,
                   parsedBuffer []string,
                   topic string,
                   sortBuffer SortFields) {

    sortBuffer(parsedBuffer)

    // Create topic if needed
    CreateTopic(producer, topic)

    for _, element := range parsedBuffer {
        Push(producer, &topic, element)
    }

    // Wait for all messages to be delivered
    producer.Flush(utils.GetInt(conf["producer.wait.time"]))
}