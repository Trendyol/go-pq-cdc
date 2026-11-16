GOBIN_DIR := $(shell go env GOPATH)/bin

default: init

.PHONY: init
init: init/lint

.PHONY: init/lint  init/vulnCheck
init/lint:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.37.0

.PHONY: init/vulnCheck
init/vulnCheck:
	go install golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: audit
audit: vendor
	@echo 'Formatting code...'
	$(GOBIN_DIR)/fieldalignment -fix ./...
	$(GOBIN_DIR)/golangci-lint run -c .golangci.yml -v --fix
	@echo 'Vetting code...'
	go vet ./...
	@echo 'Vulnerability scanning...'
	govulncheck ./...

.PHONY: tidy
tidy:
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy -compat=1.25
	go mod verify

.PHONY: tidy/all
tidy/all:
	go mod tidy
	cd example/elasticsearch && go mod tidy && cd ../..
	cd example/kafka && go mod tidy && cd ../..
	cd example/postgresql && go mod tidy && cd ../..
	cd example/simple && go mod tidy && cd ../..
	cd example/simple-file-config && go mod tidy && cd ../..
	cd integration_test && go mod tidy && cd ../
	cd benchmark/go-pq-cdc-kafka && go mod tidy && cd ../..

.PHONY: test/integration
test/integration:
	cd integration_test && go test -race -p=1 -v ./...

# test/integration/linux runs the integration suite in a Linux container on the
# Docker host's network, so the tests reach containers at their Docker network
# IP (TestReplicaGuardDNS needs that; Docker Desktop does not route container
# IPs to a macOS host). INTEGRATION_CONTAINER_IPS makes the suite reach every
# container that way; CI=true makes TestReplicaGuardDNS fail instead of skipping.
# Narrow the run with RUN, e.g. make test/integration/linux RUN=TestReplicaGuardDNS
RUN ?=
.PHONY: test/integration/linux
test/integration/linux:
	docker run --rm --network host \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v "$(CURDIR)":/src -w /src/integration_test \
		-v go-pq-cdc-gomod:/go/pkg/mod -v go-pq-cdc-gocache:/root/.cache/go-build \
		-e CI=true -e INTEGRATION_CONTAINER_IPS=true -e POSTGRES_VERSION \
		golang:1.25 go test -race -count=1 -p=1 -v -run '$(RUN)' ./...

.PHONY: lint
lint: init/lint
	@echo 'Formatting code...'
	$(GOBIN_DIR)/fieldalignment -fix ./...
	$(GOBIN_DIR)/golangci-lint run -c .golangci.yml -v --fix

.PHONY: build
build/linux:
	GOOS=linux CGO_ENABLED=0 GOARCH=amd64 go build -trimpath -a -v