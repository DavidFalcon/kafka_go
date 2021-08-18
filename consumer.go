package main

import (
    "os"
    "fmt"
    "time"
    "sync"
    "sort"
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
    connector.ProcessFields(producer, conf, parsedBuffer, "id", func (buffer []utils.RecordValue) {
        sort.SliceStable(parsedBuffer, func(i, j int) bool {
            return parsedBuffer[i].Id < parsedBuffer[j].Id
        })
    })

    connector.ProcessFields(producer, conf, parsedBuffer, "name", func (buffer []utils.RecordValue) {
        sort.SliceStable(parsedBuffer, func(i, j int) bool {
            return parsedBuffer[i].Name < parsedBuffer[j].Name
        })
    })

    connector.ProcessFields(producer, conf, parsedBuffer, "address", func (buffer []utils.RecordValue) {
        sort.SliceStable(parsedBuffer, func(i, j int) bool {
            return parsedBuffer[i].Address < parsedBuffer[j].Address
        })
    })

    connector.ProcessFields(producer, conf, parsedBuffer, "continent", func (buffer []utils.RecordValue) {
        sort.SliceStable(parsedBuffer, func(i, j int) bool {
            return parsedBuffer[i].Continent < parsedBuffer[j].Continent
        })
    })

    producer.Close()
    processingElapsed := time.Since(processingTime)

    fmt.Printf("Processing took  %s\n", processingElapsed)
}