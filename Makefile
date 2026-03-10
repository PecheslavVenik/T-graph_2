.PHONY: run test up down logs smoke

run:
	./mvnw spring-boot:run -Dspring-boot.run.profiles=local

test:
	./mvnw -q test

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f graph-api

smoke:
	./scripts/smoke.sh

