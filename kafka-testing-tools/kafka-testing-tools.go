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
	"github.com/rcrowley/go-metrics"
)

//testing
//consumerOnly
//producerOnly
//endToEnd
//multipleConsumers
//multipleProducers

var (
	testVariant  = flag.String("test-type", "end-to-end", "performace test type, available: end-to-end, producer-only, consumer-only, producer-multiple, consumer-multiple")
	messageLoad  = flag.Int("message-load", 0, "number of messages that will be produced")
	messageSize  = flag.Int("message-size", 0, "approximate message size in bytes that will be produced")
	brokers      = flag.String("brokers", "my-kafka-0.my-kafka-headless.default.svc.cluster.local:9092,my-kafka-1.my-kafka-headless.default.svc.cluster.local:9092,my-kafka-2.my-kafka-headless.default.svc.cluster.local:9092", "list of brokers")
	topic        = flag.String("topic", "my-kafka", "kafka topic name")
	throughput   = flag.Int("throughput", -1, "maximum messages sent per second")
	partition    = flag.Int("partition", -1, "partition of topic to run performance tests on")
	producerLoad = flag.Int("producer-load", 1, "number of concurrent producers")
	consumerLoad = flag.Int("consumer-load", 1, "number of concurrent consumers")
)

const (
	endToEnd         string = "end-to-end"
	producerOnly            = "producer-only"
	producerMultiple        = "producer-multiple"
	consumerOnly            = "consumer-only"
	consumerMultiple        = "consumer-multiple"
	endToEndMultiple        = "end-to-end-multiple"
)

type asyncTesting struct {
	completed chan int
	wg        gosync.WaitGroup
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
				printMetricsProducer(config.MetricRegistry)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	brokers := strings.Split(*brokers, ",")

	//run test type
	t := &asyncTesting{
		completed: make(chan int),
	}
	switch *testVariant {
	case endToEnd:
		fmt.Println("not yet implemented")
		return
	case endToEndMultiple:
		fmt.Println("not yet implemented")
		return
	case producerOnly:
		t.runProducer(*topic, *partition, *messageLoad, *messageSize, config, brokers, *throughput, *testVariant)
	case producerMultiple:
		for i := 0; i < *producerLoad; i++ {
			t.wg.Add(1)
			go t.runProducer(*topic, *partition, *messageLoad, *messageSize, config, brokers, *throughput, *testVariant)
		}
		t.wg.Wait()
	case consumerOnly:
		fmt.Println("not yet implemented")
		return
	case consumerMultiple:
		fmt.Println("not yet implemented")
		return
	}

	cancel()
	<-timer

	printMetricsProducer(config.MetricRegistry)
}

func (t *asyncTesting) runProducer(topic string, partition, messageLoad, messageSize int, config *sarama.Config, brokers []string, throughput int, testVariant string) {
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		printErrAndExit(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			printErrAndExit(err)
		}
	}()

	messages, err := generateRandomMessages(topic, partition, messageLoad, messageSize)
	if err != nil {
		printErrAndExit(err)
	}

	go func() {
		for i := 0; i < messageLoad; i++ {
			select {
			case <-producer.Successes():
				//actually not necessary
			case err := <-producer.Errors():
				fmt.Println("4")
				printErrAndExit(err)
			}
		}
		t.completed <- messageLoad
	}()

	for _, message := range messages {
		producer.Input() <- message
	}
	<-t.completed
	close(t.completed)

	//inform that goroutine finished only in multiple mode
	if testVariant == producerMultiple || testVariant == endToEndMultiple {
		t.wg.Done()
	}
}

func runConsumer() {}

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

func printMetricsProducer(r metrics.Registry) {
	recordSendRateMetric := r.Get("record-send-rate")
	requestLatencyMetric := r.Get("request-latency-in-ms")
	outgoingByteRateMetric := r.Get("outgoing-byte-rate")
	requestsInFlightMetric := r.Get("requests-in-flight")

	if recordSendRateMetric == nil || requestLatencyMetric == nil || outgoingByteRateMetric == nil ||
		requestsInFlightMetric == nil {
		return
	}
	recordSendRate := recordSendRateMetric.(metrics.Meter).Snapshot()
	requestLatency := requestLatencyMetric.(metrics.Histogram).Snapshot()
	requestLatencyPercentiles := requestLatency.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
	outgoingByteRate := outgoingByteRateMetric.(metrics.Meter).Snapshot()
	requestsInFlight := requestsInFlightMetric.(metrics.Counter).Count()
	fmt.Fprintf(os.Stdout, "%d records sent, %.1f records/sec (%.2f MiB/sec ingress, %.2f MiB/sec egress), "+
		"%.1f ms avg latency, %.1f ms stddev, %.1f ms 50th, %.1f ms 75th, "+
		"%.1f ms 95th, %.1f ms 99th, %.1f ms 99.9th, %d total req. in flight\n",
		recordSendRate.Count(),
		recordSendRate.RateMean(),
		recordSendRate.RateMean()*float64(*messageSize)/1024/1024,
		outgoingByteRate.RateMean()/1024/1024,
		requestLatency.Mean(),
		requestLatency.StdDev(),
		requestLatencyPercentiles[0],
		requestLatencyPercentiles[1],
		requestLatencyPercentiles[2],
		requestLatencyPercentiles[3],
		requestLatencyPercentiles[4],
		requestsInFlight,
	)
	fmt.Fprintf(os.Stdout, "number of concurrent producers: %d\n", *producerLoad)
}
