.PHONY: build
build:
	go build -o keep3r ./cmd/keep3r/main.go

.PHONY: run
run:
	./keep3r

.PHONY: fmt
fmt:
	go fmt ./

.PHONY: test
test:
	go test ./

.PHONY: clean
clean:
	rm ./keep3r

.DEFAULT_GOAL = build