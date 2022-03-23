package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

//Measurements gathers all metrics for producer (builtin) and consumer
type Measurements struct {
	consumerLatency []int64
	lock            sync.Mutex
	index           int64
	msgPerSec       float64
	registry        *metrics.Registry
}

func (m *Measurements) printMetricsProducer(testVariant string) {
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
	if testVariant == endToEnd || testVariant == endToEndMultiple {
		fmt.Fprintf(os.Stdout, "number of concurrent producers: %d\n", *endToEndLoad)
	} else {
		fmt.Fprintf(os.Stdout, "number of concurrent producers: %d\n", *producerLoad)
	}
}

func (m *Measurements) printMetricsConsumer(testVariant string) {
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

	fmt.Fprintf(os.Stdout, "%d records received, %.1f records/sec (%.2f MiB/sec ingress), %f ms avg latency\n",
		m.index,
		m.msgPerSec,
		// m.msgPerSec*float64(*messageSize)/1024/1024,
		incomingByteRate.RateMean()/1024/1024,
		float64(avgLatency)/float64(time.Millisecond),
	)
	if testVariant == endToEnd || testVariant == endToEndMultiple {
		fmt.Fprintf(os.Stdout, "number of concurrent consumers: %d\n", *endToEndLoad)
	} else {
		fmt.Fprintf(os.Stdout, "number of concurrent consumers: %d\n", *consumerLoad)
	}
}

func modeInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
