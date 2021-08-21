# See:
# http://www.gnu.org/software/make/manual/make.html
# http://linuxlib.ru/prog/make_379_manual.html

### Commands
build_producer:
	go build producer.go

build_consumer:
	go build consumer.go

clean:
	rm -f producer consumer

all: build_producer build_consumer

rebuild:
	docker-compose --compatibility down && docker-compose --compatibility build && docker-compose --compatibility up -d && docker-compose logs -f

