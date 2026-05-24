make:
	docker-compose down -v
	docker-compose build
	docker-compose up
detached:
	docker-compose down -v
	docker-compose build
	docker-compose up -d
stop:
	docker-compose down -v