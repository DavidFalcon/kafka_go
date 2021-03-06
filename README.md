# Mini project of using Kafka and Go

## Download

run

```
git clone https://github.com/DavidFalcon/kafka_go.git
```


## Run docker-compose


run

```
make rebuild
```
<!-- The current information can be checked in turn in kafdrop
UI Available on http://localhost:9000 -->

## Go run
### Producer
run
```
docker exec -ti go_app /go/src/app/producer/producer
```
### Consumer
run
```
docker exec -ti go_app /go/src/app/consumer/consumer
```
They both can handle input parameters, -f `config_file` and -t `topic`, by default `config_file`=../basic_config.cfg, `topic`=source

## Limitations

Whole docker-compose runs under limits CPU = 4, RAM = 4Gb
Consumer and Producer use by 1 thread - main. Consumer allocate array of stings with size 50000000

## Architecture
### Producer
The producer generates random strings in CVS format using `utils` and inserts them

### Consumer
The consumer reads data from the topic source into one data array

## Better architecture
![kafka_go](https://user-images.githubusercontent.com/17788343/130334069-cff739b9-c0b4-4c81-8b65-cf7948411399.jpg)

## Points to improve
 * Attach threads to cores to avoid cache miss
 * Use `hugepages`(linux) for direct allocating memory for consumer
 * We can use  Map Reduce approach for sorting data between nodes
 
## Time
Each application print time after completion
In my virtual machine producer takes about 25 minutes, the consumer obtains all data in 30 minutes and then sort and push in 50 minutes

## Verify 
Docker-compose runs under limits CPU = 4, RAM = 4Gb

Producer and Consumer use by 1 thread, so the whole pipeline takes only 4 cores. Consumer allocates the only array of string with size 50000000

