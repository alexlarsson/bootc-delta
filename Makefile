.PHONY: build clean test fmt

build:
	go build -o oci-delta ./cmd/oci-delta

clean:
	rm -f oci-delta

test: build
	go test ./...
	python3 tools/test-synthetic.py

fmt:
	go fmt ./...

install:
	go install ./cmd/oci-delta
