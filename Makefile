.PHONY: test test-race test-cover

test-models:
	go clean -testcache
	go test -p 1 -v ./internal/postgres
	docker compose -f test.docker-compose.yaml down -v 

test:
	go clean -testcache
	go test -p 1 -v ./
	docker compose -f test.docker-compose.yaml down -v 

test-race:
	go test -p 1 -race ./...
	docker compose -f test.docker-compose.yaml down -v 

test-cover:
	go test -p 1 -cover ./...
	docker compose -f test.docker-compose.yaml down -v
