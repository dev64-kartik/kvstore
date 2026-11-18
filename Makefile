.PHONY: all build vet lint test check proto proto-check

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

# Regenerate api/kvpb from kv.proto. Generated code is committed, so `go build` never needs the toolchain.
proto:
	go tool buf format -w
	go tool buf lint
	go tool buf generate

# What CI runs: the proto is formatted and lint-clean, and the committed generated code is exactly what the
# pinned generators produce. Fails if `make proto` would change anything.
proto-check:
	go tool buf format -d --exit-code
	go tool buf lint
	go tool buf generate
	git diff --exit-code -- api/
	test -z "$$(git status --porcelain -- api/)"

check: build vet lint test
