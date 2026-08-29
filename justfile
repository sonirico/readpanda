set shell := ["bash", "-cu"]

default:
    @just --list

build:
    go build -o bin/readpanda ./cmd/readpanda

# Build a fully static, stripped binary and install it under $GOBIN (or
# $GOPATH/bin, or ~/go/bin). The binary embeds no CGO and trimmed paths so it
# is portable across machines of the same OS/arch.
install:
    #!/usr/bin/env bash
    set -euo pipefail
    target="${GOBIN:-${GOPATH:-$HOME/go}/bin}"
    mkdir -p "$target"
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w -extldflags "-static"' \
        -o "$target/readpanda" ./cmd/readpanda
    echo "installed: $target/readpanda"
    case ":$PATH:" in
        *":$target:"*) ;;
        *) echo "warning: $target is not in your PATH" >&2 ;;
    esac

run *ARGS:
    go run ./cmd/readpanda {{ARGS}}

test:
    go test ./...

test-race:
    go test -race ./...

lint:
    golangci-lint run ./...

# Install the dev tooling this repo expects (goimports, golines,
# golangci-lint). Idempotent — re-runs upgrade in place.
setup:
    go install golang.org/x/tools/cmd/goimports@latest
    go install github.com/segmentio/golines@latest
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

fmt:
    gofmt -s -w .
    goimports -w .
    golines -w --max-len=100 --base-formatter=gofmt .

tidy:
    go mod tidy

clean:
    rm -rf bin/

demo-up:
    docker compose -f demo/docker-compose.yml up -d
    demo/seed.sh

demo-traffic:
    go run ./demo/traffic

demo-down:
    docker compose -f demo/docker-compose.yml down -v

# Requires vhs installed, and `just demo-up` plus `just demo-traffic`
# running in another terminal.
gifs:
    #!/usr/bin/env bash
    set -euo pipefail
    for tape in demo/tapes/*.tape; do
        [ -e "$tape" ] || continue
        vhs "$tape"
    done
