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

	"github.com/kubemq-io/kubemq-go"
)

var (
	testVariant  = flag.String("test-type", "end-to-end", "performace test type, available: end-to-end, producer-only, consumer-only, producer-multiple, consumer-multiple\nnote: consumer-only and consumer-multiple requires pre-generated messages")
	messageLoad  = flag.Int("message-load", 0, "number of messages that will be produced")
	messageSize  = flag.Int("message-size", 0, "approximate message size in bytes that will be produced")
	broker       = flag.String("broker", "localhost", "list of brokers")
	channel      = flag.String("channel", "my-mykubemq", "kubemq channel name")
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

	//config
	//create client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//init measurements
	m := &Measurements{
		consumerMetrics: make([]ClientMetrics, *consumerLoad),
		producerMetrics: make([]ClientMetrics, *producerLoad),
		cIndex:          0,
		pIndex:          0,
		cMsgPerSec:      0,
		pMsgPerSec:      0,
		cBytesPerSec:    0,
		pBytesPerSec:    0,
	}

	//run test type
	t := &asyncTesting{}

	switch *testVariant {
	case endToEnd:
		t.wg.Add(2)
		go t.runConsumer(ctx, *channel, *testVariant, *messageLoad, *messageSize, m)
		go t.runProducer(ctx, *channel, *testVariant, *messageLoad, *messageSize, m)
		t.wg.Wait()
	case endToEndMultiple:
		for i := 0; i < *endToEndLoad; i++ {
			t.wg.Add(1)
			go t.runConsumer(ctx, *channel, *testVariant, *messageLoad, *messageSize, m)
		}
		for i := 0; i < *endToEndLoad; i++ {
			t.wg.Add(1)
			go t.runProducer(ctx, *channel, *testVariant, *messageLoad, *messageSize, m)
		}
		t.wg.Wait()
	case producerOnly:
		t.runProducer(ctx, *channel, *testVariant, *messageLoad, *messageSize, m)
	case producerMultiple:
		for i := 0; i < *producerLoad; i++ {
			t.wg.Add(1)
			go t.runProducer(ctx, *channel, *testVariant, *messageLoad, *messageSize, m)
		}
		t.wg.Wait()
	case consumerOnly:
		t.runConsumer(ctx, *channel, *testVariant, *messageLoad, *messageSize, m)
	case consumerMultiple:
		for i := 0; i < *consumerLoad; i++ {
			t.wg.Add(1)
			go t.runConsumer(ctx, *channel, *testVariant, *messageLoad, *messageSize, m)
		}
		t.wg.Wait()
	}

	//print metrics
	m.printMetricsConsumer(*testVariant)
	m.printMetricsProducer(*testVariant)
}

func (t *asyncTesting) runProducer(ctx context.Context, channel, testVariant string, messageLoad, messageSize int, m *Measurements) {
	//inform that goroutine finished only in multiple mode
	if testVariant == producerMultiple || testVariant == endToEnd || testVariant == endToEndMultiple {
		defer t.wg.Done()
	}

	//create producer
	cli, err := kubemq.NewQueuesStreamClient(ctx,
		kubemq.WithAddress(*broker, 50000),
		kubemq.WithClientId("ClientId"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
	)
	if err != nil {
		printErrAndExit(err)
	}
	defer cli.Close()

	//generate messages
	messages, err := generateRandomMessages(channel, messageLoad, messageSize)
	if err != nil {
		printErrAndExit(err)
	}

	tMsg1 := time.Now().UnixNano() / int64(time.Nanosecond)
	for i := 0; i < len(messages); i++ {
		t1 := time.Now().UnixNano() / int64(time.Nanosecond)
		_, err = cli.Send(ctx, messages[i])
		t2 := time.Now().UnixNano() / int64(time.Nanosecond)
		m.lock.Lock()
		m.producerMetrics[m.pIndex].latencies = append(m.producerMetrics[m.pIndex].latencies, t2-t1)
		m.lock.Unlock()
	}
	tMsg2 := time.Now().UnixNano() / int64(time.Nanosecond)

	if err != nil {
		printErrAndExit(err)
	}

	m.lock.Lock()
	m.producerMetrics[m.pIndex].msgNo = messageLoad
	//count msg per sec
	if m.pMsgPerSec != 0 {
		m.pMsgPerSec = (m.pMsgPerSec + float64(messageLoad)/((float64(tMsg2)-float64(tMsg1))/float64(time.Second))) / 2
	} else {
		m.pMsgPerSec = float64(messageLoad) / ((float64(tMsg2) - float64(tMsg1)) / float64(time.Second))
	}
	//count bytes per sec
	if m.pBytesPerSec != 0 {
		m.pBytesPerSec = (m.pBytesPerSec + (float64(messageSize*messageLoad) / ((float64(tMsg2) - float64(tMsg1)) / float64(time.Second)))) / 2
	} else {
		m.pBytesPerSec = float64(messageSize*messageLoad) / ((float64(tMsg2) - float64(tMsg1)) / float64(time.Second))
	}
	m.pIndex++
	m.lock.Unlock()
}

func (t *asyncTesting) runConsumer(ctx context.Context, channel, testVariant string, messageLoad, messageSize int, m *Measurements) {
	//inform that goroutine finished only in multiple mode
	if testVariant == consumerMultiple || testVariant == endToEnd || testVariant == endToEndMultiple {
		defer t.wg.Done()
	}

	cli, err := kubemq.NewQueuesStreamClient(ctx,
		kubemq.WithAddress(*broker, 50000),
		kubemq.WithClientId("ClientId"),
		kubemq.WithAutoReconnect(true),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		printErrAndExit(err)
	}
	defer cli.Close()

	t1 := time.Now().UnixNano() / int64(time.Nanosecond)
	done, err := cli.Pull(ctx, kubemq.NewReceiveQueueMessagesRequest().SetChannel(channel).SetMaxNumberOfMessages(messageLoad).SetWaitTimeSeconds(1))
	t2 := time.Now().UnixNano() / int64(time.Nanosecond)

	if err != nil {
		printErrAndExit(err)
	}

	if len(done.Messages) < messageLoad {
		printErrAndExit(fmt.Errorf("not enough messages in queue"))
	}

	m.lock.Lock()
	m.consumerMetrics[m.cIndex].latency = t2 - t1
	m.consumerMetrics[m.cIndex].msgNo = messageLoad
	//count msg per sec
	if m.cMsgPerSec != 0 {
		m.cMsgPerSec = (m.cMsgPerSec + float64(messageLoad)/((float64(t2)-float64(t1))/float64(time.Second))) / 2
	} else {
		m.cMsgPerSec = float64(messageLoad) / ((float64(t2) - float64(t1)) / float64(time.Second))
	}
	//count bytes per sec
	if m.cBytesPerSec != 0 {
		m.cBytesPerSec = (m.cBytesPerSec + (float64(done.Messages[0].Size()*messageLoad) / ((float64(t2) - float64(t1)) / float64(time.Second)))) / 2
	} else {
		m.cBytesPerSec = float64(done.Messages[0].Size()*messageLoad) / ((float64(t2) - float64(t1)) / float64(time.Second))
	}
	m.cIndex++
	m.lock.Unlock()
}

func generateRandomMessages(channel string, messageLoad, messageSize int) ([]*kubemq.QueueMessage, error) {
	messages := make([]*kubemq.QueueMessage, messageLoad)
	for i := 0; i < messageLoad; i++ {
		payload := make([]byte, messageSize)
		if _, err := rand.Read(payload); err != nil {
			return nil, err
		}
		messages[i] = kubemq.NewQueueMessage().SetChannel(channel).SetBody(payload)
	}
	return messages, nil
}

func printErrAndExit(err error) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err)
	os.Exit(1)
}
