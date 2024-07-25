package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aserto-dev/go-authorizer/aserto/authorizer/v2/api"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ProcessorMessage struct {
	Time time.Time
}

func main() {
	// Connect to NATS
	tenantId1 := "tenant1"
	tenantId2 := "tenant2"
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)

	defer nc.Drain()

	// Creates JetStreamContext
	js, err := nc.JetStream()
	checkErr(err)

	start := 0
	for {

		log.Printf("Creating next batch of decisions: [%d] for [%s]\n", start, tenantId1)
		err = createDecision(js, start, tenantId1)
		checkErr(err)
		log.Printf("Creating next batch of decisions: [%d] for [%s]", start, tenantId2)
		err = createDecision(js, start, tenantId2)
		checkErr(err)
		start += 10
		time.Sleep(1 * time.Second)
	}
}

// createDecision publishes stream of decisions
func createDecision(js nats.JetStreamContext, start int, tenantId string) error {
	var decision api.Decision
	for i := start; i < start+10; i++ {
		decision = api.Decision{
			Id:        fmt.Sprintf("%s-%d", tenantId, i), // uuid.New().String(),
			Timestamp: timestamppb.New(time.Now()),
			Policy: &api.DecisionPolicy{
				PolicyInstance: &api.PolicyInstance{
					Name:          fmt.Sprintf("%s-%s", "policy", tenantId),
					InstanceLabel: fmt.Sprintf("%s-%s", "policy", tenantId),
				},
			},
			TenantId: &tenantId,
		}

		stream := fmt.Sprintf("ems-v2-%s", *decision.TenantId)
		streamSubject := fmt.Sprintf("%s.v2.decisions.*.*", *decision.TenantId)
		msgSubject := fmt.Sprintf("%s.v2.decisions.%s.%s", *decision.TenantId, decision.Policy.PolicyInstance.Name, decision.Id)

		streamInfo, err := createStream(js, stream, streamSubject)
		checkErr(err)

		if streamInfo != nil {
			// fmt.Printf("Publishing v2 decision %s to stream %s subject %s with msg subject %s", decision.Id, streamInfo.Config.Name, streamSubject, msgSubject)

			buf, err := proto.Marshal(
				&decision)
			checkErr(err)

			fmt.Printf("[%s] - Decision to publish: %s %s\n", tenantId, decision.Id, decision.Timestamp.AsTime())

			_, err = js.Publish(msgSubject, buf, nats.MsgId(decision.Id), nats.ExpectStream(stream))
			checkErr(err)
			time.Sleep(400 * time.Millisecond)
		}
	}
	return nil
}

// createStream creates a stream by using JetStreamContext
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
