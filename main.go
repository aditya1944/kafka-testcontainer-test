package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

type MessageConsumer interface {
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	Poll(int) kafka.Event
	CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
	Seek(tp kafka.TopicPartition, ignoredTimeoutMs int) error
	Close() error
}

var (
	kafkaBrokers = flag.String("kafkabrokers", "", "comma separated list of kafka brokers url")
	groupID      = flag.String("groupid", "", "consumer group id")
	sigs         = make(chan os.Signal, 1)
)

func main() {
	flag.Parse()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  *kafkaBrokers,
		"group.id":           *groupID,
		"auto.offset.reset":  "earliest", // start consuming from latest offset
		"enable.auto.commit": false,      // disable auto commit
	})

	if err != nil {
		log.Fatalf("consumer creation failed : %v", err)
	}

	if err := c.SubscribeTopics([]string{"myTopic"}, nil); err != nil {
		log.Fatalf("unable to subscribe")
	}

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	run := true
	go func() {
		<-sigs
		log.Printf("initiating shutdown")
		run = false
	}()
	process(&run, c)
}

func process(run *bool, m MessageConsumer) {
	for *run {
		ev := m.Poll(500) // this is blocking call and waits for atmost 0.5 second
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case kafka.Error:
			// client will try to recover from all errors
			if e.IsFatal() {
				log.Printf("fatal consumer error: %v", e)
				break
			}
			log.Printf("recoverable consumer error: %v", e)
		case *kafka.Message:
			if err := handle(e); err != nil {
				log.Printf("message not processed, offset : %d", e.TopicPartition.Offset)
				if err := m.Seek(e.TopicPartition, -1); err != nil {
					log.Printf("seek failed, err: %v", err)
					break
				}
			}
			if _, err := m.CommitMessage(e); err != nil {
				log.Printf("commiting message failed, partition: %d, offset: %d err: %v", e.TopicPartition.Partition, e.TopicPartition.Offset, err)
				break
			}
		}
	}
	if err := m.Close(); err != nil {
		log.Printf("consumer close failed, err: %v", err)
	}
}

func handle(msg *kafka.Message) error {
	ctx := context.Background()
	p := Person{}
	if err := proto.Unmarshal(msg.Value, &p); err != nil {
		log.Printf("incorrect body format, err: %v", err)
		return nil // allow message to be commited
	}
	if err := handler(ctx, &p); err != nil { // err is nil here
		return err
	}
	return nil
}
