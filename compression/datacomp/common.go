package datacompadaptive

/*
#cgo CFLAGS: -Wall -std=gnu99
#include <sched.h>
// 获取当前线程的CPU编号
int getcpu() {
    return sched_getcpu();
}
*/
// import "C"
import "github.com/wqshr12345/golib/common"

// CPU负载级别
const (
	Cpuload0 uint8 = iota // 1-33
	Cpuload1              // 34-66
	Cpuload2              // 67-99
	Cpuload3              // 100
)

// CPU频率级别
const (
	Cpufreq0 uint8 = iota // 1-25
	Cpufreq1              // 26-49
	Cpufreq2              // 50-74
	Cpufreq3              // 75-95
	Cpufreq4              // 96-100
)

// 传输带宽级别
const (
	Abw0 uint8 = iota // 1Kbit/s-768Kbit/s
	Abw1              // 768Kbit/s-2Mbit/s
	Abw2              // 2Mbit/s-20Mbit/s
	Abw3              // 20Mbit/s-200Mbit/s
	Abw4              // >200Mbit/s
)

// 字节计数级别
const (
	Bc0 uint8 = iota // >100
	Bc1              // 67-99
	Bc2              // 34-66
	Bc3              // 1-33
)

type MapKey struct {
	cpuLoad  uint8
	cpuFreq  uint8
	abw      uint8
	bc       uint8
	cmprType int
}

type MapValue struct {
	averageEOR uint32
	dataCounts uint32
	totalEOR   uint32
}

type PredicTable map[MapKey]*MapValue

func NewPredicTable() *PredicTable {
	predicTable := PredicTable(make(map[MapKey]*MapValue))
	return &predicTable
}

// 根据当前情况，获得compression type.
func (p *PredicTable) GetCmprType(cpuLoad, cpuFreq, abw, bc uint8) int {
	cmprTime := common.CompressTypeNone
	maxAverageEOR := uint32(0)
	maxCmprType := 0
	// 暂时只选择none、snappy、lz4和zstd
	for cmprTime < common.CompressTypeGzip {
		mapKey := MapKey{cpuLoad, cpuFreq, abw, bc, cmprTime}
		// 一旦有某个cmprType没有数据，或者某个cmprType有数据，但是dataCounts小于2，就用这个cmprType作为预测结果
		if value, ok := (*p)[mapKey]; !ok {
			(*p)[mapKey] = &MapValue{0, 0, 0}
			return cmprTime
		} else {
			if value.dataCounts < 2 {
				return cmprTime
			}
			if value.averageEOR > maxAverageEOR {
				maxAverageEOR = value.averageEOR
				maxCmprType = cmprTime
			}
		}
		cmprTime++
	}
	return maxCmprType
}

func (p *PredicTable) UpdateEOR(cpuLoad, cpuFreq, abw, bc uint8, cmprType int, eor uint32) {
	mapKey := MapKey{cpuLoad, cpuFreq, abw, bc, cmprType}
	if value, ok := (*p)[mapKey]; !ok {
		(*p)[mapKey] = &MapValue{eor, 1, eor}
	} else {
		value.dataCounts++
		// 根据论文所述，当计数到达20，总和EOR减去平均EOR
		if value.dataCounts == 20 {
			value.totalEOR -= value.averageEOR
		}
		value.totalEOR += eor
		value.averageEOR = value.totalEOR / value.dataCounts
		// (*p)[mapKey] = value 不需要这一行代码，因为map的value存储的是指针。
	}
}
