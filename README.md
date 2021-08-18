# Mini project of using Kafka and Go

## Install


Setup & Build Project 

```
make rebuild
```
The current information can be checked in turn in kafdrop
UI Available on http://localhost:9000

## Go installation

To be able to connect Go to the Kafka queue, you need to install the library
```
go get gopkg.in/confluentinc/confluent-kafka-go.v1/kafka
```

## Go build

Just run command

```
make all
```
After it consumer and producer will be builded

## Go run
### Producer
```
./producer
```
### Consumer
```
./consumer
```
They both can handle input parameters, -f `config_file` and -t `topic`, by default `config_file`=./basic_config.cfg, `topic`=source

## Architecture
### Producer
The producer generates random strings in CVS format using `utils` and inserts them into two partitions in parallel. Added additional thread for data insertion

### Consumer
The consumer reads data from two partitions into one data array in two streams, splitting it in half to avoid blocking for writing. There is also parsing of CVS lines for further sorting. Then the sorting by one field goes sequentially and the original string is saved, but in the sorted order.

## Points to improve
 * Attach threads to cores to avoid cache miss
 * Use `hugepages`(linux) for direct allocating memory for consumer
 * We can use  Map Reduce approach for sorting date between nodes
 
## Time
In my virtual machine producer takes about 25 minutes, consumer obtains all data in 30 minutes and then sort and push in 50 minutes


