package main

import (
    "os"
    "fmt"
    "time"
    "sort"
    "strings"
    "syscall"
    "os/signal"
    "app/utils"
    "app/connector"
    "app/config_reader"
)

func main() {

	// Initialization
	configFile, topic := config_reader.ParseArgs()
	conf := config_reader.ReadConfig(*configFile)

	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    // Allocate buffer for input date
    maxCount     := utils.GetInt(conf["date.count"])
    parsedBuffer := make([]string, maxCount)

    consumerTime := time.Now()
	// Process messages
    connector.PullMessages(topic,
                           parsedBuffer,
                           sigchan,
                           conf)

    consumerElapsed := time.Since(consumerTime)
    fmt.Printf("Consumer took  %s\n", consumerElapsed)

    processingTime := time.Now()
    // Create Producer instance
    producer := connector.CreateProducer(conf)

    connector.ProcessFields(producer, conf, parsedBuffer, "id", func (buffer []string) {
        sort.SliceStable(parsedBuffer, func(i, j int) bool {
            left := strings.Split(parsedBuffer[i], ",")
            right := strings.Split(parsedBuffer[j], ",")
            return left[0] < right[0]
            })
    })

    connector.ProcessFields(producer, conf, parsedBuffer, "name", func (buffer []string) {
        sort.SliceStable(parsedBuffer, func(i, j int) bool {
            left := strings.Split(parsedBuffer[i], ",")
            right := strings.Split(parsedBuffer[j], ",")
            return left[1] < right[1]
            })
    })

    connector.ProcessFields(producer, conf, parsedBuffer, "address", func (buffer []string) {
        sort.SliceStable(parsedBuffer, func(i, j int) bool {
            left := strings.Split(parsedBuffer[i], ",")
            right := strings.Split(parsedBuffer[j], ",")
            return left[2] < right[2]
            })
    })

    connector.ProcessFields(producer, conf, parsedBuffer, "continent", func (buffer []string) {
        sort.SliceStable(parsedBuffer, func(i, j int) bool {
            left := strings.Split(parsedBuffer[i], ",")
            right := strings.Split(parsedBuffer[j], ",")
            return left[3] < right[3]
            })
    })

    producer.Close()
    processingElapsed := time.Since(processingTime)

    fmt.Printf("Processing took  %s\n", processingElapsed)

}