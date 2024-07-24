package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type ProcessorMessage struct {
	Time time.Time
}

func main() {
	// Connect to NATS
	nc, _ := nats.Connect(nats.DefaultURL)
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	for {
		timestamp := time.Now().Add(-30 * time.Second)
		triggerFlush(js, "tenant1", timestamp)
		time.Sleep(time.Since(timestamp))
		timestamp = time.Now().Add(-30 * time.Second)
		triggerFlush(js, "tenant2", timestamp)
	}
}

func triggerFlush(js nats.JetStreamContext, tenantId string, timestamp time.Time) {
	stream := "processor"
	streamSubject := "flush.*"
	streamSubjectMsg := fmt.Sprintf("flush.%s", tenantId)
	log.Printf("\n\n\n FLUSHING: %s %s %s\n\n\n", stream, streamSubjectMsg, timestamp.UTC())

	createStream(js, stream, streamSubject)
	processMessage := ProcessorMessage{
		Time: timestamp,
	}

	buf, err := json.Marshal(processMessage)
	checkErr(err)
	checkErr(err)

	_, err = js.Publish(streamSubjectMsg, buf, nats.ExpectStream(stream))
	log.Printf("published: %s\n", streamSubjectMsg)
	checkErr(err)
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
			Name:     streamName,
			Subjects: []string{streamSubjects},
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
