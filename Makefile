.PHONY: run test up down logs smoke bench bench-data bench-suite bench-backends

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

bench:
	./scripts/bench.sh

bench-data:
	./scripts/bench-data.sh

bench-suite:
	./scripts/bench-suite.sh

bench-backends:
	./scripts/bench-suite.sh
