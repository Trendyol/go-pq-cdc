default: init

.PHONY: init
init: init/lint

.PHONY: init/lint  init/vulnCheck
init/lint:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.1
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.22.0

.PHONY: init/vulnCheck
init/vulnCheck:
	go install golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: audit
audit: vendor
	@echo 'Formatting code...'
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix
	@echo 'Vetting code...'
	go vet ./...
	@echo 'Vulnerability scanning...'
	govulncheck ./...

.PHONY: tidy
tidy:
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy -compat=1.22.4
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

.PHONY: lint
lint: init/lint
	@echo 'Formatting code...'
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

.PHONY: build
build/linux:
	GOOS=linux CGO_ENABLED=0 GOARCH=amd64 go build -trimpath -a -v