package main

import (
	"fmt"
	"sort"
	"time"
)

//Stats are statistics used for analysis of performance
type Stats struct {
	// 	lock sync.Mutex

	// 	units   string           // the units of the measurement (used for pretty printing)
	// 	binsize float64          // size of the bins used for accumulating counts
	// 	counts  map[int64]uint64 // maps bin number to the count of how many x values fell into that bin

	// 	n        int     // # of times x was accumulated
	// 	sum      float64 // sum of all x
	// 	min, max float64
	start              int64
	windowStart        int64
	latencies          []int
	sampling           int
	iteration          int
	index              int64
	count              int64
	bytes              int64
	maxLatency         int64
	totalLatency       int64
	windowCount        int64
	windowMaxLatency   int64
	windowTotalLatency int64
	windowBytes        int64
	reportingInterval  int
}

func (s *Stats) init(numRecords int64, reportingInterval int) {
	s.start = time.Now().UnixNano() / int64(time.Millisecond) //time will be returned in ms
	s.windowStart = time.Now().UnixNano() / int64(time.Millisecond)
	s.iteration = 0
	s.sampling = int(numRecords / min(numRecords, 500000))
	s.latencies = make([]int, int(numRecords)/s.sampling+1)
	s.index = 0
	s.maxLatency = 0
	s.totalLatency = 0
	s.windowCount = 0
	s.windowMaxLatency = 0
	s.windowTotalLatency = 0
	s.windowBytes = 0
	s.totalLatency = 0
	s.reportingInterval = reportingInterval
}

func (s *Stats) record(iter int, latency int, bytes int64, time int64) {
	s.count++
	s.bytes += bytes
	s.totalLatency += int64(latency)
	s.maxLatency = max(s.maxLatency, int64(latency))
	s.windowCount++
	s.windowBytes += bytes
	s.windowTotalLatency += int64(latency)
	s.windowMaxLatency = max(s.windowMaxLatency, int64(latency))
	if iter%s.sampling == 0 {
		s.latencies[s.index] = latency
		s.index++
	}
	/* maybe report the recent perf */
	if time-s.windowStart >= int64(s.reportingInterval) {
		s.printWindow()
		s.newWindow()
	}
}

func (s *Stats) printWindow() {
	var elapsed = time.Now().UnixNano()/int64(time.Millisecond) - s.windowStart
	var recsPerSec = float64(1000.0 * s.windowCount / elapsed)
	var mbPerSec = float64(1000.0 * s.windowBytes / elapsed / (1024.0 * 1024.0))
	fmt.Printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %d ms max latency./n",
		s.windowCount,
		recsPerSec,
		mbPerSec,
		float64(s.windowTotalLatency/s.windowCount),
		s.windowMaxLatency)
}

func (s *Stats) newWindow() {
	s.windowStart = time.Now().UnixNano() / int64(time.Millisecond)
	s.windowCount = 0
	s.windowMaxLatency = 0
	s.windowTotalLatency = 0
	s.windowBytes = 0
}

func (s *Stats) printMeasurements() {
	var elapsed = time.Now().UnixNano()/int64(time.Millisecond) - s.start
	var recsPerSec = float64(1000.0 * s.count / elapsed)
	var mbPerSec = float64(1000.0 * s.bytes / elapsed / (1024.0 * 1024.0))
	percs := s.percentiles(s.latencies, s.index, 0.5, 0.95, 0.99, 0.999)
	fmt.Printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
		s.count,
		recsPerSec,
		mbPerSec,
		s.totalLatency/s.count,
		s.maxLatency,
		percs[0],
		percs[1],
		percs[2],
		percs[3])
}

func (s *Stats) percentiles(latencies []int, count int64, percentiles ...float64) []int {
	size := min(count, int64(len(latencies)))
	sort.Ints(latencies)
	values := make([]int, len(percentiles))
	for i := 0; i < len(percentiles); i++ {
		var index = int((percentiles[i] * float64(size)))
		values[i] = latencies[index]
	}
	return values
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
