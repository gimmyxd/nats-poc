package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aserto-dev/go-events/aserto/events/packager/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	// Connect to NATS
	nc, err := nats.Connect("nats://localhost:4222",
		nats.ClientCert(
			"/Users/gimmy/.config/nats/certs/nats.tls.crt",
			"/Users/gimmy/.config/nats/certs/nats.tls.key",
		),
		nats.RootCAs("/Users/gimmy/.config/nats/certs/nats.ca.crt"))
	if err != nil {
		log.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// url := os.Getenv("NATS_URL")
	// if url == "" {
	// 	url = nats.DefaultURL
	// }

	// nc, _ := nats.Connect(url)
	// defer nc.Drain()

	// js, err := nc.JetStream()
	// checkErr(err)

	for {
		triggerFlush(ctx, js, "45bfc282-1533-11ec-9980-00e16a9c7735", "policy-rebac")
		time.Sleep(10 * time.Second)
		triggerFlush(ctx, js, "45bfc282-1533-11ec-9980-00e16a9c7735", "todo")
		time.Sleep(10 * time.Second)
	}
}

func triggerFlush(ctx context.Context, js jetstream.JetStream, tenantId string, policyName string) {
	msgSubject := fmt.Sprintf("grpc.aserto.events.decision.v2.DecisionEvent.%s.%s", tenantId, policyName)

	tenantStreamName := fmt.Sprintf("ems-v2-%s", tenantId)
	js.DeleteConsumer(ctx, tenantStreamName, "processor")

	dur, err := js.CreateOrUpdateConsumer(ctx, tenantStreamName, jetstream.ConsumerConfig{
		Durable:       "processor",
		FilterSubject: msgSubject,
	})
	checkErr(err)
	stream := "packager"
	streamSubject := "grpc.aserto.events.packager.v2.PackagerEvent.*.*"
	streamSubjectMsg := fmt.Sprintf("grpc.aserto.events.packager.v2.PackagerEvent.%s.%s", tenantId, policyName)
	createStream(ctx, js, stream, streamSubject, jetstream.WorkQueuePolicy)

	var remaining uint64
	timestamp := time.Now()
	remaining = uint64(dur.CachedInfo().NumPending)

	js.DeleteConsumer(ctx, tenantStreamName, "processor")

	log.Printf("[%s][%s]: remaining: %d", tenantId, policyName, remaining)

	processMessage := packager.PackagerEvent{
		TenantId:   tenantId,
		PolicyName: policyName,
		Bucket:     packager.Bucket_S3,
		MsgCount:   remaining,
		CreatedAt:  timestamppb.New(dur.CachedInfo().TimeStamp),
	}

	buf, err := proto.Marshal(&processMessage)
	checkErr(err)

	if remaining > 1 {
		log.Printf("[%s]: flushing [%s]: %s %s %s\n", tenantId, policyName, stream, streamSubjectMsg, timestamp.UTC())
		_, err = js.Publish(ctx, streamSubjectMsg, buf, jetstream.WithExpectStream(stream))
		checkErr(err)
	} else {
		log.Printf("no events to flush")
	}

	checkErr(err)
}

// func getTenantStreamInfo(jsCtx nats.JetStreamContext, tenantId string, subject string) *nats.StreamInfo {
// 	streamName := fmt.Sprintf("ems-v2-%s", tenantId)
// 	stream, err := jsCtx.StreamInfo(streamName)
// 	checkErr(err)

// 	return stream
// }

func createStream(ctx context.Context, js jetstream.JetStream, streamName string, streamSubjects string, policy jetstream.RetentionPolicy) (jetstream.Stream, error) {
	// Check if the DEMO_EVENT stream already exists; if not, create it.
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		log.Println(err)
	}
	if stream == nil {
		log.Printf("creating stream %q and subjects %q \n", streamName, streamSubjects)
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:      streamName,
			Retention: policy,
			Subjects:  []string{streamSubjects},
		})
		if err != nil {
			return nil, err
		}
	}
	return stream, nil
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
