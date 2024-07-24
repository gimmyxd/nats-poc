package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aserto-dev/go-authorizer/aserto/authorizer/v2/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type ProcessorMessage struct {
	Time time.Time
}

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 1000*time.Second)

	jsx, err := jetstream.New(nc)
	checkErr(err)

	_, err = js.StreamInfo("processor")
	if err != nil {
		log.Fatal(err)
	}
	// Create durable consumer monitor
	js.Subscribe("flush.*", func(msg *nats.Msg) {
		msg.Ack()
		var processMessage ProcessorMessage
		err := json.Unmarshal(msg.Data, &processMessage)
		checkErr(err)

		log.Printf("processMessage: %v", processMessage)

		tenantId := strings.Split(msg.Subject, ".")[1]
		log.Printf("processing :%s\n", tenantId)
		flushEvents(ctx, jsx, tenantId, processMessage)
	}, nats.Durable("monitor"), nats.ManualAck())

	// defer cancel()

	runtime.Goexit()
}

func flushEvents(ctx context.Context, js jetstream.JetStream, tenantId string, processMessage ProcessorMessage) {
	log.Printf("flusing events for: %s\n", tenantId)

	streamName := fmt.Sprintf("ems-v2-%s", tenantId)

	stream, err := js.Stream(ctx, streamName)
	checkErr(err)

	remaining := stream.CachedInfo().State.Msgs
	log.Printf("remaining: %d\n", remaining)

	dur, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       streamName,
		OptStartTime:  &processMessage.Time,
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
	})

	checkErr(errors.Wrap(err, "failed to create durable stream"))

	var seq jetstream.SequencePair
	fetchResult, _ := dur.Fetch(100, jetstream.FetchMaxWait(100*time.Millisecond))
	for msg := range fetchResult.Messages() {
		msg.Ack()
		// data := msg.Subject()
		var decision api.Decision
		err := proto.Unmarshal(msg.Data(), &decision)
		checkErr(err)

		meta, err := msg.Metadata()
		checkErr(err)
		seq = meta.Sequence
		writeEventsToFile(&decision, tenantId, processMessage.Time)
	}

	log.Printf("seq: %v", seq)
	purgeOpts := []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject(fmt.Sprintf("%s.v2.decisions.*.*", tenantId)), jetstream.WithPurgeSequence(seq.Stream)}

	stream.Purge(ctx, purgeOpts...)

	stream.DeleteConsumer(ctx, streamName)
}

// writeEventsToFile reviews the event and publishes EVENTS.approved event
func writeEventsToFile(decision *api.Decision, tenantId string, time time.Time) {
	eventJSON, _ := json.Marshal(decision)

	dir, err := os.Getwd()
	checkErr(err)
	file := fmt.Sprintf("%s/data/%s_%s_log.txt", dir, tenantId, time.UTC())
	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)

	checkErr(err)

	defer f.Close()

	_, err = f.Write(eventJSON)
	checkErr(err)

	_, err = f.WriteString("\n")
	checkErr(err)
	// log.Printf("done writing events to [%s]", file)
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
