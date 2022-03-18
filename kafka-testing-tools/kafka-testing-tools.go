package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	gosync "sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
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
	completed chan int
	wg        gosync.WaitGroup
}

type measurements struct {
	consumerLatency []int64
	lock            sync.Mutex
	index           int64
	msgPerSec       float64
	registry        *metrics.Registry
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

	//prepare measurements for consumer
	m := &measurements{
		consumerLatency: make([]int64, *messageLoad**consumerLoad),
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
	close(t.completed)

	m.printMetricsProducer(*testVariant)
	m.printMetricsConsumer(*testVariant)
}

func (t *asyncTesting) runProducer(topic string, partition, messageLoad, messageSize int, config *sarama.Config, brokers []string, throughput int, testVariant string) {
	//inform that goroutine finished only in multiple mode
	if testVariant == producerMultiple || testVariant == endToEndMultiple {
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
}

func (t *asyncTesting) runConsumer(cli sarama.Client, topic string, partition, messageLoad int, offset int64, m *measurements, testVariant string) {
	//inform that goroutine finished only in multiple mode
	if testVariant == consumerMultiple || testVariant == endToEndMultiple {
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
	m.lock.Lock()
	m.msgPerSec = (m.msgPerSec + float64(m.index)/(tMsg2-tMsg1)) / 2
	m.lock.Unlock()
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

func (m *measurements) printMetricsProducer(testVariant string) {
	r := *m.registry
	recordSendRateMetric := r.Get("record-send-rate")
	requestLatencyMetric := r.Get("request-latency-in-ms")
	outgoingByteRateMetric := r.Get("outgoing-byte-rate")
	requestsInFlightMetric := r.Get("requests-in-flight")

	if recordSendRateMetric == nil || requestLatencyMetric == nil || outgoingByteRateMetric == nil ||
		requestsInFlightMetric == nil {
		return
	}
	allowedModes := []string{producerOnly, producerMultiple, endToEnd, endToEndMultiple}
	if !modeInSlice(testVariant, allowedModes) {
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

func (m *measurements) printMetricsConsumer(testVariant string) {
	r := *m.registry
	incomingByteRateMetric := r.Get("incoming-byte-rate")

	allowedModes := []string{consumerOnly, consumerMultiple, endToEnd, endToEndMultiple}
	if !modeInSlice(testVariant, allowedModes) {
		return
	}

	var avgLatency int64 = 0
	for i := 0; i < len(m.consumerLatency); i++ {
		avgLatency += m.consumerLatency[i]
	}
	avgLatency = avgLatency / int64(len(m.consumerLatency))
	incomingByteRate := incomingByteRateMetric.(metrics.Meter).Snapshot()

	fmt.Fprintf(os.Stdout, "%d records received, %.1f records/sec (%.2f MiB/sec ingress, %.2f MiB/sec egress), %f ms avg latency\n",
		m.index,
		m.msgPerSec,
		m.msgPerSec*float64(*messageSize)/1024/1024,
		incomingByteRate.RateMean()/1024/1024,
		float64(avgLatency)/float64(time.Millisecond),
	)
	fmt.Fprintf(os.Stdout, "number of concurrent consumers: %d\n", *consumerLoad)
}

func modeInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
