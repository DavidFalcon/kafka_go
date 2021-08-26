# See:
# http://www.gnu.org/software/make/manual/make.html
# http://linuxlib.ru/prog/make_379_manual.html

rebuild:
	docker-compose --compatibility down && docker-compose --compatibility build && docker-compose --compatibility up -d && docker-compose logs -f

