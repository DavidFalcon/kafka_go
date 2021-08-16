package main

import (
    "./connector"
    "./config_reader"
    "fmt"
    "os/signal"
    "syscall"
    "os"
    "time"
)

func main() {

	// Initialization
	configFile, topic := config_reader.ParseArgs()
	conf := config_reader.ReadConfig(*configFile)

	// Create Consumer instance
    consumer:= connector.CreateConsumer(conf)

	// Subscribe to topic
	consumer.SubscribeTopics([]string{*topic}, nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	totalCount := 0
	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(5000 * time.Millisecond)
			if err != nil {
                // The client will automatically try to recover from all errors.
                fmt.Printf("Consumer error: %v (%v)\n", err, msg)
//                 consumer.Close()
//                 // Create Consumer instance
//                 consumer:= connector.CreateConsumer(conf)
//
//                 // Subscribe to topic
//                 consumer.SubscribeTopics([]string{*topic}, nil)
				continue
			}
			recordValue := msg.Value
			totalCount += 1
			fmt.Printf("Consumed record value %s, and updated total count to %d\n", recordValue, totalCount)
		}
	}

	fmt.Printf("Closing consumer\n")
	consumer.Close()

}