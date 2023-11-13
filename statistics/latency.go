package statistics

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)

func NewLatencyStatistics(output *os.File) *LatencyStatistics {
	return &LatencyStatistics{
		count:       0,
		latencyData: []time.Duration{},
		output:      io.Writer(output),
	}
}

type LatencyStatistics struct {
	latencyData []time.Duration
	count       uint32
	output      io.Writer
}

func (ls *LatencyStatistics) AddLatency(latency time.Duration) {
	ls.latencyData = append(ls.latencyData, latency)
	ls.count++
}

func (ls *LatencyStatistics) SetOutputFile(file *os.File) {
	ls.output = io.Writer(file)
}

func (ls *LatencyStatistics) CalcAndPrintLatency() {
	sum := int64(0)
	for _, d := range ls.latencyData {
		sum += d.Nanoseconds()
	}
	elapsed := time.Duration(sum / int64(ls.count))
	sort.SliceStable(ls.latencyData, func(i, j int) bool {
		return ls.latencyData[i] < ls.latencyData[j]
	})
	//
	// Special handling for ls.count == 1. This prevents negative index
	// in the latencyNumber index. The other option is to use
	// roundUpToZero() but that is more expensive.
	//
	countFixed := ls.count
	if countFixed == 1 {
		countFixed = 2
	}
	avg := elapsed
	min := ls.latencyData[0]
	max := ls.latencyData[ls.count-1]
	p50 := ls.latencyData[((countFixed*50)/100)-1]
	p90 := ls.latencyData[((countFixed*90)/100)-1]
	p95 := ls.latencyData[((countFixed*95)/100)-1]
	p99 := ls.latencyData[((countFixed*99)/100)-1]
	p999 := ls.latencyData[uint64(((float64(countFixed)*99.9)/100)-1)]
	p9999 := ls.latencyData[uint64(((float64(countFixed)*99.99)/100)-1)]
	ls.emitLatencyResults(avg, min, max, p50, p90, p95, p99, p999, p9999)
}

func (ls *LatencyStatistics) emitLatencyHdr() {
	s := []string{"Avg", "Min", "50%", "90%", "95%", "99%", "99.9%", "99.99%", "Max"}
	ls.output.Write([]byte(("-----------------------------------------------------------------------------------------\n")))
	ls.output.Write([]byte(fmt.Sprintf("%9s %9s %9s %9s %9s %9s %9s %9s %9s\n", s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8])))
}

func (ls *LatencyStatistics) emitLatencyResults(avg, min, max, p50, p90, p95, p99, p999, p9999 time.Duration) {
	ls.emitLatencyHdr()
	ls.output.Write([]byte(fmt.Sprintf("%9s %9s %9s %9s %9s %9s %9s %9s %9s\n",
		durationToString(avg), durationToString(min),
		durationToString(p50), durationToString(p90),
		durationToString(p95), durationToString(p99),
		durationToString(p999), durationToString(p9999),
		durationToString(max))))
}

func durationToString(d time.Duration) string {
	if d < 0 {
		return d.String()
	}
	ud := uint64(d)
	val := float64(ud)
	unit := ""
	if ud < uint64(60*time.Second) {
		switch {
		case ud < uint64(time.Microsecond):
			unit = "ns"
		case ud < uint64(time.Millisecond):
			val = val / 1000
			unit = "us"
		case ud < uint64(time.Second):
			val = val / (1000 * 1000)
			unit = "ms"
		default:
			val = val / (1000 * 1000 * 1000)
			unit = "s"
		}

		result := strconv.FormatFloat(val, 'f', 3, 64)
		return result + unit
	}

	return d.String()
}
