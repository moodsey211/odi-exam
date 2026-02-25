build:
	docker build -t fastapi-dev:latest -f docker/images/fastapi-dev src/app

up:
	docker-compose run --service-ports api bash

start:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f api