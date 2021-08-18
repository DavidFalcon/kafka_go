package main

import (
    "fmt"
    "time"
    "sync"
    "./rand"
    "./utils"
    "./connector"
    "./config_reader"
)

func main() {

    // Initialization
    configFile, topic := config_reader.ParseArgs()
    conf := config_reader.ReadConfig(*configFile)

    // Run generator
    rand.RandInit()

    maxCount := utils.GetInt(conf["date.count"])
    start    := time.Now()
    // Generate random date and push
    //multithreaded producing
    //create only 1 new thread because the thread limit is 4
    // 2 thread for consumer, 1 is main and 1 additional
    var wg sync.WaitGroup
    wg.Add(1)
    go connector.PushMessages(&wg, conf, topic, 0, maxCount/2)
    connector.PushMessages(nil, conf, topic, 1, maxCount/2)
    wg.Wait()

    elapsed := time.Since(start)
    fmt.Printf("Produce took  %s\n", elapsed)
}