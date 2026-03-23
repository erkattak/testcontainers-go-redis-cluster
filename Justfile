#!/usr/bin/env just

set shell := ["zsh", "-cu"]
set dotenv-filename := ".env"
set dotenv-load := true
set quiet := true

# Display a list of available recipes
default:
    just --list

# Run tests with the race detector
test:
    go test -race ./...

lint:
    golangci-lint run

fmt:
    golangci-lint fmt
