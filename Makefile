GO ?= go

.PHONY: fmt vet test build run

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

build:
	$(GO) build ./...

run:
	$(GO) run ./main.go
