package main

import (
    "./connector"
    "./config_reader"
    "./rand"
    "fmt"
    "strconv"
    "os"
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

    rand.RandInit()
    // Generate random date and push
    for n := 0; n < count; n++ {
        fmt.Printf("Push #%d\n", n)
        connector.Push(producer, topic, rand.RandRecord())
    }

    // Wait for all messages to be delivered
    producer.Flush(15 * 1000)


}