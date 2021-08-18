package main

import (
    "os"
    "fmt"
    "time"
    "sync"
    "syscall"
    "os/signal"
    "./utils"
    "./connector"
    "./config_reader"
)

func main() {

	// Initialization
	configFile, topic := config_reader.ParseArgs()
	conf := config_reader.ReadConfig(*configFile)

	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    // Allocate buffer for input date
    maxCount        := utils.GetInt(conf["date.count"])
    parsedBuffer    := make([]utils.RecordValue, maxCount)

    consumerTime := time.Now()
	// Process messages
    //multithreaded consuming
    //create only 1 new thread because the thread limit is 4
    // 2 thread for producer, 1 is main and 1 additional
    var wg sync.WaitGroup
    wg.Add(1)
    go connector.PullMessages(&wg,
                              topic,
                              0,
                              parsedBuffer,
                              sigchan,
                              conf,
                              0,
                              maxCount/2)
    connector.PullMessages(nil,
                           topic,
                           1,
                           parsedBuffer,
                           sigchan,
                           conf,
                           maxCount/2,
                           maxCount)
    wg.Wait()

    consumerElapsed := time.Since(consumerTime)
    fmt.Printf("Consumer took  %s\n", consumerElapsed)

    processingTime := time.Now()
    // Create Producer instance
    producer := connector.CreateProducer(conf)
    connector.ProcessId(producer, conf, parsedBuffer, "id")
    connector.ProcessName(producer, conf, parsedBuffer, "name")
    connector.ProcessAddress(producer, conf, parsedBuffer, "address")
    connector.ProcessContinent(producer, conf, parsedBuffer, "continent")
    producer.Close()
    processingElapsed := time.Since(processingTime)

    fmt.Printf("Processing took  %s\n", processingElapsed)
}