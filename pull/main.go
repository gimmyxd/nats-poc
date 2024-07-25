package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aserto-dev/go-authorizer/aserto/authorizer/v2/api"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type ProcessorMessage struct {
	Time     time.Time
	FirstSeq uint64
	LastSeq  uint64
	Count    uint64
}

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	checkErr(err)

	_, err = js.StreamInfo("processor")
	if err != nil {
		log.Fatal(err)
	}
	// Create durable consumer processor
	js.Subscribe("flush.*", func(msg *nats.Msg) {
		msg.Ack()
		var processMessage ProcessorMessage
		err := json.Unmarshal(msg.Data, &processMessage)
		checkErr(err)

		tenantId := strings.Split(msg.Subject, ".")[1]
		log.Printf("[%s]: processing\n", tenantId)
		flushEvents(js, tenantId, processMessage)
	}, nats.Durable("processor"), nats.ManualAck())

	runtime.Goexit()
}

func flushEvents(js nats.JetStreamContext, tenantId string, processMessage ProcessorMessage) {
	log.Printf("[%s]: flusing events", tenantId)

	streamName := fmt.Sprintf("ems-v2-%s", tenantId)

	remaining := processMessage.Count
	firstSeq := processMessage.FirstSeq
	lastSeq := processMessage.LastSeq
	log.Printf("[%s]: remaining: %d, firstSeq: %d, lastSeq: %d\n", tenantId, remaining, firstSeq, lastSeq)

	dur, err := js.PullSubscribe(fmt.Sprintf("%s.v2.decisions.*.*", tenantId), streamName,
		nats.StartSequence(firstSeq),
	)

	checkErr(errors.Wrap(err, "failed to create durable stream"))

	if remaining > 0 {
		fetchResult, err := dur.Fetch(int(remaining), nats.MaxWait((100 * time.Millisecond)))
		checkErr(err)
		for _, msg := range fetchResult {
			msg.Ack()
			var decision api.Decision
			err := proto.Unmarshal(msg.Data, &decision)
			checkErr(err)
			meta, err := msg.Metadata()
			checkErr(err)
			if meta.NumPending != 0 {
				writeEventsToFile(&decision, tenantId, processMessage.Time)
			}
		}
	}

	log.Printf("[%s]: purging to: %v", tenantId, lastSeq)

	js.PurgeStream(streamName, &nats.StreamPurgeRequest{
		Sequence: lastSeq,
		Keep:     0,
		Subject:  fmt.Sprintf("%s.v2.decisions.*.*", tenantId),
	})

	js.DeleteConsumer(streamName, streamName)
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
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
