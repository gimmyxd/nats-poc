module github.com/aserto-dev/nats-poc

go 1.22.2

replace github.com/aserto-dev/go-events => ../go-events

require (
	github.com/nats-io/nats.go v1.36.0
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/aserto-dev/go-grpc v0.8.68 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	golang.org/x/text v0.17.0 // indirect
)

require (
	github.com/aserto-dev/go-authorizer v0.20.8
	github.com/aserto-dev/go-events v0.0.6-0.20240812115846-ec4d35cc27ce
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pkg/errors v0.9.1
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/sys v0.24.0 // indirect
)
