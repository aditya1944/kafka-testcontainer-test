package main

import (
	"context"
	"flag"
	"io"
	"strconv"
	"strings"
	"syscall"

	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"google.golang.org/protobuf/proto"
)

func TestMain(t *testing.T) {
	ctx := context.Background()

	c, err := kafka.RunContainer(ctx, kafka.WithClusterID("test-cluster"),
		testcontainers.WithConfigModifier(func(config *container.Config) {
			config.Env = append(config.Env, "KAFKA_OFFSETS_RETENTION_MINUTES: 1")
		}))
	if err != nil {
		t.Fatal(err)
	}

	// defer kafkaContainer.Terminate(ctx)
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Set the KAFKA_BROKERS environment variable to the IP and port of the Kafka container
	flag.Set("kafkabrokers", brokers[0])
	flag.Set("groupid", "myGroup")

	produceKafkaMessage(t, brokers[0])

	go func() {
		main()
	}()

	// verify that the message was consumed
	time.Sleep(2 * time.Second)
	verifyMessageConsumed(t, ctx, c)
	sigs <- syscall.SIGINT
}

func verifyMessageConsumed(t *testing.T, ctx context.Context, c *kafka.KafkaContainer) {	t.Helper()
	_, outputReader, err := c.Exec(ctx, []string{"bash", "-c", "/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --group myGroup --describe | awk 'NR>1 {print $4}'"})

	if err != nil {
		t.Fatal(err)
	}

	outputBytes, err := io.ReadAll(outputReader)
	if err != nil {
		t.Fatal(err)
	}
	output := string(outputBytes)

	// Split the output into lines
	lines := strings.Split(output, "\n")

	// The last line is empty because of the trailing newline, so get the second last line
	lastLine := lines[len(lines)-2]

	// Parse the last line as an integer
	offset, err := strconv.Atoi(lastLine)
	if err != nil {
		t.Fatal(err)
	}

	if offset != 1 {
		t.Errorf("message not consumed")
	}
}

func produceKafkaMessage(t *testing.T, kafkaServer string) {
	t.Helper()
	kafkaProducer, err := k.NewProducer(&k.ConfigMap{
		"bootstrap.servers": kafkaServer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer kafkaProducer.Close()

	topic, deliveryChan := "myTopic", make(chan k.Event)
	if err = kafkaProducer.Produce(&k.Message{
		TopicPartition: k.TopicPartition{Topic: &topic, Partition: k.PartitionAny},
		Value:          buildMessage(t),
		Key:            []byte("key"),
	}, deliveryChan); err != nil {
		t.Fatal(err)
	}

	e := <-deliveryChan
	_, ok := e.(*k.Message)
	if !ok {
		t.Fatal("message not produced")
	}
}

func buildMessage(t *testing.T) []byte {
	p := &Person{
		Name: "John Doe",
	}
	data, err := proto.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
