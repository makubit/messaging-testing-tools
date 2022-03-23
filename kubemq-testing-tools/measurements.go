//Measurements gathers all metrics for producer (builtin) and consumer
package main

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

type Measurements struct {
	consumerMetrics []ClientMetrics
	producerMetrics []ClientMetrics
	lock            sync.Mutex
	cIndex          int64
	pIndex          int64
	cMsgPerSec      float64
	pMsgPerSec      float64
	cBytesPerSec    float64
	pBytesPerSec    float64
}

type ClientMetrics struct {
	msgNo       int
	latencies   []int64 //in nanoseconds
	latency     int64
	bytesPerSec float64
}

func (m *Measurements) printMetricsProducer(testVariant string) {
	allowedModes := []string{producerOnly, producerMultiple, endToEnd, endToEndMultiple}
	if !modeInSlice(testVariant, allowedModes) {
		return
	}

	var msgReceived int = 0
	for i := 0; i < len(m.producerMetrics); i++ {
		msgReceived += m.producerMetrics[i].msgNo
	}

	var avgLatency float64 = 0
	var size int = 0
	for i := 0; i < len(m.producerMetrics); i++ {
		for j := 0; j < len(m.producerMetrics[i].latencies); j++ {
			avgLatency += float64((m.producerMetrics[i].latencies[j]))
		}
		size += len(m.producerMetrics[i].latencies)
	}
	avgLatency = float64(avgLatency) / float64(size)

	latencyPercentiles := percentiles(m.producerMetrics, size, 0.5, 0.75, 0.95, 0.99, 0.999)

	fmt.Fprintf(os.Stdout, "%d records sent, %.1f records/sec (%.6f MiB/sec egress), "+
		"%.4f ms avg latency, %.1f ms 50th, %.1f ms 75th, "+
		"%.1f ms 95th, %.1f ms 99th, %.1f ms 99.9th\n",
		msgReceived,
		m.pMsgPerSec,
		m.pBytesPerSec/1024/1024,
		avgLatency/float64(time.Millisecond),
		latencyPercentiles[0]/float64(time.Millisecond),
		latencyPercentiles[1]/float64(time.Millisecond),
		latencyPercentiles[2]/float64(time.Millisecond),
		latencyPercentiles[3]/float64(time.Millisecond),
		latencyPercentiles[4]/float64(time.Millisecond),
	)
	if testVariant == endToEnd || testVariant == endToEndMultiple {
		fmt.Fprintf(os.Stdout, "number of concurrent producers: %d\n", *endToEndLoad)
	} else {
		fmt.Fprintf(os.Stdout, "number of concurrent producers: %d\n", *producerLoad)
	}
}

func (m *Measurements) printMetricsConsumer(testVariant string) {
	allowedModes := []string{consumerOnly, consumerMultiple, endToEnd, endToEndMultiple}
	if !modeInSlice(testVariant, allowedModes) {
		return
	}

	var avgLatency int64 = 0
	for i := 0; i < len(m.consumerMetrics); i++ {
		avgLatency += (m.consumerMetrics[i].latency / int64(m.consumerMetrics[i].msgNo))
	}
	avgLatency = avgLatency / int64(len(m.consumerMetrics))

	var msgReceived int = 0
	for i := 0; i < len(m.consumerMetrics); i++ {
		msgReceived += m.consumerMetrics[i].msgNo
	}

	fmt.Fprintf(os.Stdout, "%d records received, %.1f records/sec (%.2f MiB/sec ingress), %f ms avg latency\n",
		msgReceived,
		m.cMsgPerSec,
		m.cBytesPerSec/1024/1024,
		float64(avgLatency)/float64(time.Millisecond),
	)
	if testVariant == endToEnd || testVariant == endToEndMultiple {
		fmt.Fprintf(os.Stdout, "number of concurrent consumers: %d\n", *endToEndLoad)
	} else {
		fmt.Fprintf(os.Stdout, "number of concurrent consumers: %d\n", *consumerLoad)
	}
}

func percentiles(metrics []ClientMetrics, count int, percentiles ...float64) []float64 {
	var latencies []int
	for i := 0; i < len(metrics); i++ {
		for j := 0; j < len(metrics[i].latencies); j++ {
			latencies = append(latencies, int(metrics[i].latencies[j]))
		}
	}

	size := min(count, len(latencies))
	sort.Ints(latencies)
	values := make([]float64, len(percentiles))
	for i := 0; i < len(percentiles); i++ {
		var index = int((percentiles[i] * float64(size)))
		values[i] = float64(latencies[index])
	}
	return values
}

func modeInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
