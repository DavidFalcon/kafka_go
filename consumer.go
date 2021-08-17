package main

import (
    "./connector"
    "./config_reader"
    "./utils"
    "os/signal"
    "fmt"
    "syscall"
    "os"
    "time"
    "sync"
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


    // Allocate buffers for input date
    maxCount        := utils.GetInt(conf["date.count"])
    inputBuffer     := make([]string, maxCount)
    idBuffer        := make([]utils.SortedIntRef, maxCount)
    nameBuffer      := make([]utils.SortedStringRef, maxCount)
    addressBuffer   := make([]utils.SortedStringRef, maxCount)
    continentBuffer := make([]utils.SortedStringRef, maxCount)

    consumerTime := time.Now()
	// Process messages
    connector.PullMessages(consumer,
                           inputBuffer,
                           idBuffer,
                           nameBuffer,
                           addressBuffer,
                           continentBuffer,
                           sigchan,
                           conf)

	fmt.Printf("Closing consumer\n")
	consumer.Close()
    consumerElapsed := time.Since(consumerTime)
    fmt.Printf("Consumer took  %s\n", consumerElapsed)

    processingTime := time.Now()
    //multithreaded queue processing
    //create only 3 new threads because the thread limit is 4
    var wg sync.WaitGroup
    wg.Add(3)
    go connector.ProcessStringField(&wg, conf, nameBuffer, "name")
    go connector.ProcessStringField(&wg, conf, addressBuffer, "address")
    go connector.ProcessStringField(&wg, conf, continentBuffer, "continent")
    connector.ProcessIntField(conf, idBuffer, "id")
    wg.Wait()

    processingElapsed := time.Since(processingTime)
    fmt.Printf("Processing took  %s\n", processingElapsed)
}