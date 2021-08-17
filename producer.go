package main

import (
    "fmt"
    "time"
    "./connector"
    "./config_reader"
    "./rand"
    "./utils"
)

func main() {

	// Initialization
	configFile, topic := config_reader.ParseArgs()
	conf := config_reader.ReadConfig(*configFile)

    // Get Producer instance
    producer:= connector.GetProducer(conf, topic)

    // Run generator
    rand.RandInit()

    start := time.Now()
    // Generate random date and push
    for n := 0; n < utils.GetInt(conf["date.count"]); n++ {
        fmt.Printf("Push #%d\n", n)
        connector.Push(producer, topic, rand.RandRecord())
    }

    // Wait for all messages to be delivered
    producer.Flush(utils.GetInt(conf["producer.wait.time"]))

    elapsed := time.Since(start)
    fmt.Printf("Produce took  %s\n", elapsed)

    producer.Close()
}