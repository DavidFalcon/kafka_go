package main

import (
    "./connector"
    "./config_reader"
    "./rand"
    "fmt"
    "strconv"
    "os"
    "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	// Initialization
	configFile, topic := config_reader.ParseArgs()
	conf := config_reader.ReadConfig(*configFile)

    // Create Producer instance
    producer:= connector.CreateProducer(conf)

    // Create topic if needed
    connector.CreateTopic(producer, *topic)

    count, err := strconv.Atoi(conf["date.count"])
    if err != nil {
        // handle error
        fmt.Println(err)
        os.Exit(2)
    }

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

    rand.RandInit()
    // Generate random date and push
    for n := 0; n < count; n++ {
        fmt.Printf("Push #%d\n", n)
        connector.Push(producer, topic, rand.RandRecord())
//         if n % 10000 == 0 {
//             producer.Flush(500)
//         }
    }

    // Wait for all messages to be delivered



}