.PHONY: all build vet lint test check

# `make check` is the gate before calling any change done (see CLAUDE.md).
all: check

build:
	go build ./...

vet:
	go vet ./...

lint:
	golangci-lint run

# Always with -race: most of this project is concurrent, and the race detector only finds
# races in code paths that actually execute, so it has to be on for every run.
test:
	go test -race ./...

check: build vet lint test
