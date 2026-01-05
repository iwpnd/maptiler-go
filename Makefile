VERSION ?= edge
GITSHA    := $(shell git rev-parse --short HEAD)
BUILDTIME := $(shell date +%FT%T%z)
LDFLAGS   := -X github.com/ipwnd/maptiler-go/cmd/maptiler/version.Version=$(VERSION) \
             -X github.com/iwpnd/maptiler-go/cmd/maptiler/version.GitSHA=$(GITSHA) \
             -X github.com/iwpnd/maptiler-go/cmd/maptiler/version.BuildTime=$(BUILDTIME)

GOARCH ?= amd64
CGO_ENABLED ?= 0

.PHONY: build
build:
	@echo "building"
	@echo "VERSION=$(VERSION) GITSHA=$(GITSHA) BUILDTIME=$(BUILDTIME)"
	@echo "LDFLAGS=$(LDFLAGS)"
	GOARCH=$(GOARCH) CGO_ENABLED=$(CGO_ENABLED) go build -ldflags '$(LDFLAGS)' -o ./bin/maptiler ./cmd/maptiler

.PHONY: install
install:
	@echo "Checking for version manager..."
	@if command -v mise >/dev/null 2>&1; then \
		echo "Using mise to install dependencies"; \
		mise install; \
	else \
		echo "Error: mise not installed"; \
		echo "Please install from:"; \
		echo "  • mise (recommended): https://mise.jdx.dev"; \
		exit 1; \
	fi
	@echo "Installing pre-commit hooks..."
	@uvx pre-commit install
	@uvx pre-commit install --hook-type commit-msg
	@uvx pre-commit install --hook-type prepare-commit-msg
	@echo "Installation complete!"

.PHONY: test
test:
	@echo "running tests"
	@go test -v -json ./... | tparse -all

.PHONY: lint
lint:
	@echo "running linter"
	@golangci-lint run
