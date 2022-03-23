package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"strings"
	gosync "sync"
	"time"

	"github.com/Shopify/sarama"
)

var (
	testVariant  = flag.String("test-type", "end-to-end", "performace test type, available: end-to-end, producer-only, consumer-only, producer-multiple, consumer-multiple\nnote: consumer-only and consumer-multiple requires pre-generated messages")
	messageLoad  = flag.Int("message-load", 0, "number of messages that will be produced")
	messageSize  = flag.Int("message-size", 0, "approximate message size in bytes that will be produced")
	brokers      = flag.String("brokers", "my-kafka-0.my-kafka-headless.default.svc.cluster.local:9092,my-kafka-1.my-kafka-headless.default.svc.cluster.local:9092,my-kafka-2.my-kafka-headless.default.svc.cluster.local:9092", "list of brokers")
	topic        = flag.String("topic", "my-kafka", "kafka topic name")
	throughput   = flag.Int("throughput", -1, "maximum messages sent per second")
	partition    = flag.Int("partition", 0, "partition of topic to run performance tests on")
	producerLoad = flag.Int("producer-load", 1, "number of concurrent producers")
	consumerLoad = flag.Int("consumer-load", 1, "number of concurrent consumers")
	endToEndLoad = flag.Int("end-to-end-load", 1, "number of concurrent producers and consumers")
	maxBatchSize = flag.Int("max-batch-size", 1, "max number of messages that will be polled")
	requiredAcks = flag.Int("required-acks", 1, "required number of acks needed from the broker (-1: all, 0: none, 1: local).")
	timeout      = flag.Duration("timeout", 10*time.Second, "duration the producer will wait to receive required acks.")
)

const (
	endToEnd         string = "end-to-end"
	producerOnly            = string("producer-only")
	producerMultiple        = "producer-multiple"
	consumerOnly            = "consumer-only"
	consumerMultiple        = "consumer-multiple"
	endToEndMultiple        = "end-to-end-multiple"
)

type asyncTesting struct {
	wg gosync.WaitGroup
}

func parseTestType() {
	possibleTests := map[string]struct{}{
		endToEnd:         {},
		producerOnly:     {},
		producerMultiple: {},
		consumerOnly:     {},
		consumerMultiple: {},
		endToEndMultiple: {},
	}
	_, ok := possibleTests[strings.ToLower(*testVariant)]
	if ok != true {
		printErrAndExit(fmt.Errorf("cannot parse test type: %s, please use one of: end-to-end, producer-only, producer-multiple, consumer-only, consumer-multiple, end-to-end-multiple", *testVariant))
	}
}

func main() {
	flag.Parse()
	//validate if test type is correct
	parseTestType()

	//more configs to be added
	config := sarama.NewConfig()
	config.ClientID = "testClientID"
	config.Producer.Return.Successes = true
	config.Consumer.Fetch.Max = int32(*maxBatchSize * *messageSize)
	config.Producer.RequiredAcks = sarama.RequiredAcks(*requiredAcks)
	config.Producer.Timeout = *timeout
	//testing flush?

	//prepare measurements for consumer
	var load int = 0
	if *testVariant == endToEndMultiple {
		load = *endToEndLoad
	} else {
		load = *consumerLoad
	}
	m := &Measurements{
		consumerLatency: make([]int64, *messageLoad*load),
		registry:        &config.MetricRegistry,
		index:           0,
		msgPerSec:       0,
	}

	cli, err := sarama.NewClient([]string{*brokers}, config)
	if err != nil {
		printErrAndExit(err)
	}
	defer cli.Close()

	//validate parameters before creating producer and consumer
	if err := config.Validate(); err != nil {
		printErrAndExit(err)
	}

	timer := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		defer close(timer)
		t := time.Tick(5 * time.Second)
		for {
			select {
			case <-t:
				m.printMetricsProducer(*testVariant)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	brokers := strings.Split(*brokers, ",")

	//run test type
	t := &asyncTesting{}

	switch *testVariant {
	case endToEnd:
		t.wg.Add(2)
		go t.runConsumer(cli, *topic, *partition, *messageLoad, sarama.OffsetNewest, m, *testVariant)
		go t.runProducer(*topic, *partition, *messageLoad, *messageSize, config, brokers, *throughput, *testVariant)
		t.wg.Wait()
	case endToEndMultiple:
		for i := 0; i < *endToEndLoad; i++ {
			t.wg.Add(1)
			go t.runConsumer(cli, *topic, *partition, *messageLoad, sarama.OffsetNewest, m, *testVariant)
		}
		for i := 0; i < *endToEndLoad; i++ {
			t.wg.Add(1)
			go t.runProducer(*topic, *partition, *messageLoad, *messageSize, config, brokers, *throughput, *testVariant)
		}
		t.wg.Wait()
	case producerOnly:
		t.runProducer(*topic, *partition, *messageLoad, *messageSize, config, brokers, *throughput, *testVariant)
	case producerMultiple:
		for i := 0; i < *producerLoad; i++ {
			t.wg.Add(1)
			go t.runProducer(*topic, *partition, *messageLoad, *messageSize, config, brokers, *throughput, *testVariant)
		}
		t.wg.Wait()
	case consumerOnly:
		t.runConsumer(cli, *topic, *partition, *messageLoad, sarama.OffsetOldest, m, *testVariant)
	case consumerMultiple:
		for i := 0; i < *consumerLoad; i++ {
			t.wg.Add(1)
			go t.runConsumer(cli, *topic, *partition, *messageLoad, sarama.OffsetOldest, m, *testVariant)
		}
		t.wg.Wait()
	}

	cancel()
	<-timer

	m.printMetricsProducer(*testVariant)
	m.printMetricsConsumer(*testVariant)
}

func (t *asyncTesting) runProducer(topic string, partition, messageLoad, messageSize int, config *sarama.Config, brokers []string, throughput int, testVariant string) {
	//inform that goroutine finished only in multiple mode
	if testVariant == producerMultiple || testVariant == endToEnd || testVariant == endToEndMultiple {
		defer t.wg.Done()
	}

	//create async producer
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		printErrAndExit(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			printErrAndExit(err)
		}
	}()

	//generate messages
	messages, err := generateRandomMessages(topic, partition, messageLoad, messageSize)
	if err != nil {
		printErrAndExit(err)
	}
	completed := make(chan int)

	go func() {
		for i := 0; i < messageLoad; i++ {
			select {
			case <-producer.Successes():
				//actually not necessary
			case err := <-producer.Errors():
				fmt.Println("error on receiving success from producer")
				printErrAndExit(err)
			}
		}
		completed <- messageLoad
	}()

	for _, message := range messages {
		producer.Input() <- message
	}
	<-completed
	close(completed)
}

func (t *asyncTesting) runConsumer(cli sarama.Client, topic string, partition, messageLoad int, offset int64, m *Measurements, testVariant string) {
	//inform that goroutine finished only in multiple mode
	if testVariant == consumerMultiple || testVariant == endToEnd || testVariant == endToEndMultiple {
		defer t.wg.Done()
	}

	consumer, err := sarama.NewConsumerFromClient(cli)
	if err != nil {
		printErrAndExit(err)
	}
	//range partitions if we have more partitions
	p, err := consumer.ConsumePartition(topic, int32(partition), offset)
	if err != nil {
		printErrAndExit(fmt.Errorf("error subscribing to partition: %w", err))
	}
	defer p.Close()
	tMsg1 := float64(time.Now().UnixNano()) / float64(time.Second)

	//receive messages
	for i := 0; i < messageLoad; i++ {
		t1 := time.Now().UnixNano() / int64(time.Nanosecond)
		<-p.Messages()
		t2 := time.Now().UnixNano() / int64(time.Nanosecond)
		m.lock.Lock()
		m.consumerLatency[m.index] = t2 - t1
		m.index++
		m.lock.Unlock()
	}
	tMsg2 := float64(time.Now().UnixNano()) / float64(time.Second)
	if m.msgPerSec != 0 {
		m.lock.Lock()
		m.msgPerSec = (m.msgPerSec + float64(messageLoad)/(tMsg2-tMsg1)) / 2
		m.lock.Unlock()
	} else {
		m.lock.Lock()
		m.msgPerSec = float64(messageLoad) / (tMsg2 - tMsg1)
		m.lock.Unlock()
	}

}

func generateRandomMessages(topic string, partition, messageLoad, messageSize int) ([]*sarama.ProducerMessage, error) {
	messages := make([]*sarama.ProducerMessage, messageLoad)
	for i := 0; i < messageLoad; i++ {
		payload := make([]byte, messageSize)
		if _, err := rand.Read(payload); err != nil {
			return nil, err
		}
		messages[i] = &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(partition),
			Value:     sarama.ByteEncoder(payload),
		}
	}
	return messages, nil
}

func printErrAndExit(err error) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err)
	os.Exit(1)
}
