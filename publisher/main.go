package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	streamName       = "DEMO_EVENT"
	streamSubjects   = "DEMO_EVENT.*"
	subjectName      = "DEMO_EVENT.created"
	subjectNameExtra = "DEMO_EVENT.out-of-scope"
)

type DemoEvent struct {
	ID      int
	Message string
	Status  string
}

func main() {
	// Connect to NATS
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)

	defer nc.Drain()

	// Creates JetStreamContext
	js, err := nc.JetStream()
	checkErr(err)
	// Creates stream
	err = createStream(js)
	checkErr(err)
	// Create demo_event by publishing messages
	start := 1
	for {
		log.Printf("Creating next batch of events: [%d]", start)
		err = createDemoEvent(js, start, subjectName)
		checkErr(err)
		err = createDemoEvent(js, start, subjectNameExtra)
		checkErr(err)
		start += 10
		time.Sleep(4 * time.Second)
	}
}

// createDemoEvent publishes stream of events
// with subject "DEMO_EVENT.created"
func createDemoEvent(js nats.JetStreamContext, start int, subjectName string) error {
	var demoEvent DemoEvent
	for i := start; i < start+10; i++ {
		demoEvent = DemoEvent{
			ID:      i,
			Message: fmt.Sprintf("DemoEvent: %d", i),
			Status:  "created",
		}
		demoEventJSON, _ := json.Marshal(demoEvent)
		_, err := js.Publish(subjectName, demoEventJSON)
		if err != nil {
			return err
		}
		log.Printf("DemoEvent with ID:%d has been published to [%s]\n", i, subjectName)
	}
	return nil
}

// createStream creates a stream by using JetStreamContext
func createStream(js nats.JetStreamContext) error {
	// Check if the DEMO_EVENT stream already exists; if not, create it.
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		log.Println(err)
	}
	if stream == nil {
		log.Printf("creating stream %q and subjects %q", streamName, streamSubjects)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
