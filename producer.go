package main

import (
    "fmt"
    "time"
    "./rand"
    "./connector"
    "./config_reader"
)

func main() {

    // Initialization
    configFile, topic := config_reader.ParseArgs()
    conf := config_reader.ReadConfig(*configFile)

    // Run generator
    rand.RandInit()

    start    := time.Now()

    connector.PushMessages(conf, topic)

    elapsed := time.Since(start)
    fmt.Printf("Produce took  %s\n", elapsed)
}