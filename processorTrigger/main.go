package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type ProcessorMessage struct {
	Time     time.Time
	FirstSeq uint64
	LastSeq  uint64
	Count    uint64
}

func main() {
	// Connect to NATS
	nc, _ := nats.Connect(nats.DefaultURL)
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	for {
		triggerFlush(js, "tenant1")
		time.Sleep(30 * time.Second)
		triggerFlush(js, "tenant2")
		time.Sleep(30 * time.Second)
	}
}

func triggerFlush(js nats.JetStreamContext, tenantId string) {
	tenantStream := getTenantStreamInfo(tenantId, js)

	stream := "processor"
	streamSubject := "flush.*"
	streamSubjectMsg := fmt.Sprintf("flush.%s", tenantId)

	createStream(js, stream, streamSubject)

	timestamp := time.Now()
	remaining := tenantStream.State.Msgs
	firstSeq := tenantStream.State.FirstSeq
	lastSeq := tenantStream.State.LastSeq

	log.Printf("[%s]: remaining: %d, firstSeq: %d, lastSeq: %d\n", tenantId, remaining, firstSeq, lastSeq)

	processMessage := ProcessorMessage{
		Time:     timestamp,
		FirstSeq: firstSeq,
		LastSeq:  lastSeq,
		Count:    remaining,
	}

	buf, err := json.Marshal(processMessage)
	checkErr(err)

	_, err = js.Publish(streamSubjectMsg, buf, nats.ExpectStream(stream))
	log.Printf("[%s]: flusing: %s %s %s\n", tenantId, stream, streamSubjectMsg, timestamp.UTC())
	checkErr(err)
}

func getTenantStreamInfo(tenantId string, js nats.JetStreamContext) *nats.StreamInfo {
	streamName := fmt.Sprintf("ems-v2-%s", tenantId)

	stream, err := js.StreamInfo(streamName)
	checkErr(err)

	return stream
}

func createStream(js nats.JetStreamContext, streamName string, streamSubjects string) (*nats.StreamInfo, error) {
	// Check if the DEMO_EVENT stream already exists; if not, create it.
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		log.Println(err)
	}
	if stream == nil {
		log.Printf("creating stream %q and subjects %q \n", streamName, streamSubjects)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:      streamName,
			Retention: nats.WorkQueuePolicy,
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
