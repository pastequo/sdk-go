package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"

	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

const (
	count = 100
)

func main() {

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_0_0_0
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	test := fmt.Sprintf("batch-%v", time.Now().Unix())
	topic := "demo"

	//*
	receiver, err := kafka_sarama.NewConsumer([]string{"127.0.0.1:9092"}, saramaConfig, "pitie-cg", topic)
	if err != nil {
		log.Fatalf("failed to create receiver: %s", err.Error())
	}

	defer receiver.Close(context.Background())

	c, err := cloudevents.NewClient(receiver, cloudevents.WithPollGoroutines(1))
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	// Create a done channel to block until we've received (count) messages
	done := make(chan struct{})

	// Start the receiver
	go func() {
		log.Printf("will listen consuming topic %v for test %v\n", topic, test)
		var recvCount int32
		err = c.StartReceiver(context.TODO(), func(ctx context.Context, event cloudevents.Event) {
			receive(ctx, event)
			if atomic.AddInt32(&recvCount, 1) == count {
				done <- struct{}{}
			}
		})
		if err != nil {
			log.Fatalf("failed to start receiver: %s", err)
		} else {
			log.Printf("receiver stopped\n")
		}
	}()

	// go func() {
	// 	time.Sleep(7 * time.Second)
	// 	panic(errors.New("booom"))
	// }()
	//*/

	/*
		// Ensure that the consumer is ready before staring pushing events
		time.Sleep(4 * time.Second)
	*/

	/*
		sender, err := kafka_sarama.NewSender([]string{"127.0.0.1:9092"}, saramaConfig, topic)
		if err != nil {
			log.Fatalf("failed to create protocol: %s", err.Error())
		}

		defer sender.Close(context.Background())

		cp, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs(), cloudevents.WithPollGoroutines(1))
		if err != nil {
			log.Fatalf("failed to create client, %v", err)
		}

		// Start sending the events
		for i := 0; i < count; i++ {
			e := cloudevents.NewEvent()
			e.SetType("com.cloudevents.sample.sent")
			e.SetSource("https://github.com/cloudevents/sdk-go/v2/samples/httpb/requester")
			_ = e.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
				"test": test,
				"id":   i,
			})

			if result := cp.Send(context.Background(), e); cloudevents.IsUndelivered(result) {
				log.Printf("failed to send: %v", err)
			} else {
				log.Printf("sent: %d, accepted: %t", i, cloudevents.IsACK(result))
			}
		}
		//*/

	<-done
}

func receive(ctx context.Context, event cloudevents.Event) {
	// Random processing time
	n := rand.Intn(1000)
	time.Sleep(time.Duration(n) * time.Second)

	fmt.Printf("After %vs, %s\n", n, event.DataEncoded)
}
