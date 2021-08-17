package main

import (
    "./connector"
    "./config_reader"
    "fmt"
    "os/signal"
    "syscall"
    "os"
    "time"
    "strconv"
    "strings"
    "sort"
    "sync"
)

type sorted_int struct {
    id int32
    date  *string
}
type sorted_string struct {
    name string
    date  *string
}

func handl_int(conf map[string]string, date []sorted_int, topic string) {
    sort.SliceStable(date, func(i, j int) bool {
        return date[i].id < date[j].id
    })
    // Create Producer instance
    producer:= connector.CreateProducer(conf)

    // Create topic if needed
    connector.CreateTopic(producer, topic)

    for _, element := range date {
        fmt.Printf("Push id %d, record %s\n",
                    element.id,
                    *element.date)
        connector.Push(producer, &topic, *element.date)
    }

    // Wait for all messages to be delivered
    producer.Flush(15 * 1000)
}

func handl_str(wg *sync.WaitGroup, conf map[string]string, date []sorted_string, topic string) {
    defer wg.Done()

    fmt.Printf("handl_str len befor date %d\n", len(date))

    sort.SliceStable(date, func(i, j int) bool {
        return date[i].name < date[j].name
    })

    fmt.Printf("handl_str len after date %d\n", len(date))

    // Create Producer instance
    producer:= connector.CreateProducer(conf)

    // Create topic if needed
    connector.CreateTopic(producer, topic)

    for _, element := range date {
        fmt.Printf("Push name %s, record %s\n",
                    element.name,
                    *element.date)
        connector.Push(producer, &topic, *element.date)
    }

    // Wait for all messages to be delivered
    producer.Flush(15 * 1000)
}

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
	current := 0
	run := true

    count, err := strconv.Atoi(conf["date.count"])
    if err != nil {
        // handle error
        fmt.Println(err)
        os.Exit(2)
    }

    buffer           := make([]string, count)
    buffer_id        := make([]sorted_int, count)
    buffer_name      := make([]sorted_string, count)
    buffer_address   := make([]sorted_string, count)
    buffer_continent := make([]sorted_string, count)

	for run == true && current < count {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(500 * time.Millisecond)
			if err != nil {
                // The client will automatically try to recover from all errors.
                fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				continue
			}
			buffer[current] = string(msg.Value)
			row_date := strings.Split(buffer[current], ",")
			id, err := strconv.ParseInt(row_date[0], 10, 32)
            if err != nil {
                // handle error
                fmt.Println(err)
                os.Exit(2)
            }
			buffer_id[current]        = sorted_int{int32(id), &buffer[current]}
			buffer_name[current]      = sorted_string{row_date[1], &buffer[current]}
            buffer_address[current]   = sorted_string{row_date[2], &buffer[current]}
            buffer_continent[current] = sorted_string{row_date[3], &buffer[current]}
//             fmt.Printf("Consumed record value %s, and updated total count to %d\n", buffer[current], current)
            fmt.Printf("Consumed record value id %d, name %s, address %s, continent %s\n",
                        buffer_id[current].id,
                        buffer_name[current].name,
                        buffer_address[current].name,
                        buffer_continent[current].name)
			current++
		}
	}

	fmt.Printf("Closing consumer\n")
	consumer.Close()

    fmt.Printf("len id %d, len buffer_name %d, len buffer_address %d, len buffer_continent %d\n",
                            len(buffer_id),
                            len(buffer_name),
                            len(buffer_address),
                            len(buffer_continent))

    var wg sync.WaitGroup
    wg.Add(3)
    go handl_str(&wg, conf, buffer_name, "name")
    go handl_str(&wg, conf, buffer_address, "address")
    go handl_str(&wg, conf, buffer_continent, "continent")
    handl_int(conf, buffer_id, "id")
    wg.Wait()
}