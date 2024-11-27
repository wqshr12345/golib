package rtc

import (
	"math"

	"github.com/wqshr12345/golib/compression/rtc/lpsimplex"
)

// 用于监控当前的状况，以及设置cache等...
var Moni = NewMonitor()

type Monitor struct {
	// key 是列名/数据类型名，value是对应的算法及压缩信息...
	cmpr_cache map[ColumnCmpr]CompressionInfo

	// 当前网络带宽
	net_bandwitdh float64
	// 一次压缩大小
	M float64
	//方案备选池
	rdpoint []ColumnCmpr
	//方案等待备选池(待采样)
	sppoint []ColumnCmpr
}

func NewMonitor() *Monitor {
	return &Monitor{
		cmpr_cache:    make(map[ColumnCmpr]CompressionInfo),
		net_bandwitdh: 0,
	}
}

type ColumnCmpr struct {
	Column byte // 代表列名/类型
	Cmpr   byte // 代表压缩算法
}

type CompressionInfo struct {
	CompressionGain      float64
	CompressionBandwidth float64 // 压缩带宽 bytes/s
}

type CompressionIntro struct {
	Point   ColumnCmpr
	ByteNum float64
}

const (
	BODY_LEN byte = iota
)

const (
	NO_COMPRESSION byte = 150 + iota
	SNAPPY
	GZIP
	LZ4
	ZSTD
	NO_VALID
)

// 更新Cache中的值
func (m *Monitor) SetCompressionInfo(columnCmpr ColumnCmpr, info CompressionInfo) {
	m.cmpr_cache[columnCmpr] = info
}

// 目前的更新策略选择覆盖更新
func (m *Monitor) UpdateCompressionInfo(columnCmpr ColumnCmpr, info CompressionInfo) {
	// fmt.Println("before update cache: column ", columnCmpr, "info ", info)
	m.cmpr_cache[columnCmpr] = info
	// fmt.Println("after update cache: column ", columnCmpr, "info ", info)
}

// 得到某一列使用某种方法
func (m *Monitor) GetCompressionType() {

}

// 根据当前的cache值以及网络带宽，通过最优化办法计算得到每个列的压缩算法及占比

func (m *Monitor) GetBetterResult() {

}

func (m *Monitor) Getreadysppoint(readtype []byte) {
	var readypoint []ColumnCmpr
	var samplepoint []ColumnCmpr
	for i, elem := range readtype {
		if elem == 1 {
			for j := NO_COMPRESSION; j < NO_VALID; j++ {
				c := ColumnCmpr{}
				c.Column = byte(i + 1)
				c.Cmpr = byte(j)
				if _, ok := m.cmpr_cache[c]; ok {
					readypoint = append(readypoint, c)
				} else {
					samplepoint = append(samplepoint, c)
				}

			}
		}
	}
	m.rdpoint = readypoint
	m.sppoint = samplepoint
}

func (m *Monitor) Getalpharatio() []CompressionIntro {
	length_rdpoint := len(m.rdpoint)
	length_sppoint := len(m.sppoint)
	sampleratio := float64(length_rdpoint / length_sppoint)
	sample_bytes := math.Round(m.M * sampleratio)
	ready_bytes := m.M - sample_bytes

	A := make([][]float64, 2)
	for i := 0; i < 2; i++ {
		A[i] = make([]float64, length_rdpoint)
	}
	B := []float64{m.net_bandwitdh, 1}
	C := make([]float64, length_rdpoint)
	for i, elem := range m.rdpoint {
		A[0][i] = m.cmpr_cache[elem].CompressionBandwidth
		A[0][i] = 1
		C[i] = -m.cmpr_cache[elem].CompressionGain * m.cmpr_cache[elem].CompressionBandwidth
	}
	tol := 1.0e-12
	//tol := float64(0)
	bland := false
	maxiter := 4000
	bounds := []lpsimplex.Bound{{0, math.Inf(1)}}
	//callback := LPSimplexVerboseCallback
	//callback := LPSimplexTerseCallback
	callback := lpsimplex.Callbackfunc(nil)
	disp := false //true

	solution := lpsimplex.LPSimplex(C, A, B, nil, nil, bounds, callback, disp, maxiter, tol, bland)
	alpha := solution.X
	opt := solution.Fun
	res := make([]CompressionIntro, length_rdpoint+length_sppoint)
	for i, elem := range m.rdpoint {
		res[i].ByteNum = math.Round(alpha[i] * m.cmpr_cache[elem].CompressionBandwidth * m.cmpr_cache[elem].CompressionGain * ready_bytes / (-opt))
		res[i].Point = elem
	}
	for i, elem := range m.sppoint {
		res[i+length_rdpoint].ByteNum = math.Round(sample_bytes / float64(length_sppoint))
		res[i+length_rdpoint].Point = elem
	}
	return res
}
