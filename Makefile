default: init

.PHONY: init
init:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.1
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.22.0
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

.PHONY: vendor
vendor: tidy
	@echo 'Tidying and verifying module dependencies...'
	go mod tidy -compat=1.22
	go mod verify
	@echo 'Vendoring dependencies...'
	go mod vendor

.PHONY: tidy
tidy:
	go mod tidy
	cd example/elasticsearch && go mod tidy && cd ../..
	cd example/kafka && go mod tidy && cd ../..
	cd example/postgresql && go mod tidy && cd ../..
	cd example/simple && go mod tidy && cd ../..
	cd integration_test && go mod tidy && cd ../
	cd benchmark/go-pq-cdc-kafka && go mod tidy && cd ../..

.PHONY: test/integration
test/integration:
	cd integration_test && go test -race -p=1 -v ./...

