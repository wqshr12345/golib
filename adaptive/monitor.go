package adaptive

import (
	"fmt"
	"math"
	"time"

	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/compression/rtc/lpsimplex"
)

// 用于监控当前的状况，以及设置cache等
type Monitor struct {
	// key 是列名/数据类型名，value是对应的算法及压缩信息
	cmpr_cache map[common.ColumnCmpr]common.CompressionInfo

	// 当前网络带宽 bytes/s
	net_bandwitdh float64
	timer         *time.Timer
	// 一次压缩大小
	M int
	//方案备选池
	rdpoint []common.ColumnCmpr
	//方案等待备选池(待采样)
	sppoint []common.ColumnCmpr

	test_times int64

	tempEpoch int64

	// 计算cache的阈值
	epochThreshold int64

	flag bool

	alpha []float64

	opt float64
}

func NewMonitor(packageSize int, rate float64) *Monitor {
	return &Monitor{
		cmpr_cache:     make(map[common.ColumnCmpr]common.CompressionInfo),
		net_bandwitdh:  rate, // TODOIMP 暂时用固定的...
		timer:          time.NewTimer(5 * time.Second),
		M:              packageSize,
		test_times:     0,
		tempEpoch:      0,
		epochThreshold: 50,
		flag:           false,
		opt:            0,
	}
}

func (m *Monitor) UpdateBandwidth(bw float64) {
	if (math.Abs(bw-m.net_bandwitdh) / m.net_bandwitdh) > 0.1 {
		m.flag = true
	}
	m.net_bandwitdh = bw
	m.timer.Reset(5 * time.Second)

}

// TODOIMP 并发安全问题
func (m *Monitor) ResetBandWitdh() {
	for range m.timer.C {
		m.net_bandwitdh = math.MaxFloat64
	}
}

// 目前的更新策略选择覆盖更新
func (m *Monitor) UpdateCompressionInfo(column byte, cmprtype byte, orilen int, cmprlen int, cmprtime float64) {

	cc := common.ColumnCmpr{Column: column, Cmpr: cmprtype}
	fmt.Println("before update cache: column ", column, " cmprType ", cmprtype, " cmprGain ", m.cmpr_cache[cc].CompressionGain, " cmprBw ", m.cmpr_cache[cc].CompressionBandwidth)

	if (cmprlen == 0) || (cmprtime == 0) {
		panic("cmprlen or cmprtime is zero")
	}
	cmprRatio := float64(orilen) / float64(cmprlen)
	cmprBw := float64(cmprlen) / cmprtime

	ci := common.CompressionInfo{CompressionGain: cmprRatio, CompressionBandwidth: cmprBw, Epoch: m.tempEpoch}
	if (math.Abs(ci.CompressionBandwidth-m.cmpr_cache[cc].CompressionBandwidth)/m.cmpr_cache[cc].CompressionBandwidth > 0.1) || (math.Abs(ci.CompressionGain-m.cmpr_cache[cc].CompressionGain) > 1) {
		m.flag = true
	}
	m.cmpr_cache[cc] = ci
	fmt.Println("after update cache: column ", column, " cmprType ", cmprtype, " cmprGain ", m.cmpr_cache[cc].CompressionGain, " cmprBw ", m.cmpr_cache[cc].CompressionBandwidth)
}

// 得到某一列使用某种方法
func (m *Monitor) GetCompressionType() {

}

func (m *Monitor) getReadySpPoint(offAndLen []common.OffAndLen) {
	var readypoint []common.ColumnCmpr
	var samplepoint []common.ColumnCmpr
	for i, elem := range offAndLen {
		if (elem.Len - elem.Offset) > 0 {
			for j := common.INVALID_START + 1; j < common.INVALID_END; j++ {
				c := common.ColumnCmpr{}
				c.Column = byte(i)
				c.Cmpr = byte(j)
				// 针对INT的特定操作
				if j == common.DELTA || j == common.RLE {
					continue
				}
				if (j == common.DELTA || j == common.RLE) && i != common.Int {
					continue
				}
				// flag := false
				if _, ok := m.cmpr_cache[c]; ok {
					// if m.tempEpoch-m.cmpr_cache[c].Epoch > m.epochThreshold {
					// 	samplepoint = append(samplepoint, c)
					// 	// flag = true
					// } else {
					readypoint = append(readypoint, c)
					// }
				} else {
					// flag = true
					samplepoint = append(samplepoint, c)
				}
				// if flag {
				// 	info := m.cmpr_cache[c]
				// 	info.Epoch = m.tempEpoch
				// 	m.cmpr_cache[c] = info
				// }
			}
		}
	}
	if len(readypoint) != len(m.rdpoint) {
		m.flag = true
	}

	m.rdpoint = readypoint
	m.sppoint = samplepoint
}

// 根据当前的cache值以及网络带宽，通过最优化办法计算得到每个列的压缩算法及占比
func (m *Monitor) GetAlphaRatio(offAndLen []common.OffAndLen) []common.CompressionIntro {
	newOffAndLen := make([]common.OffAndLen, len(offAndLen))
	for i, elem := range offAndLen {
		newOffAndLen[i] = elem
	}
	m.tempEpoch++
	m.getReadySpPoint(newOffAndLen)
	length_rdpoint := len(m.rdpoint)
	length_sppoint := len(m.sppoint)
	if (length_rdpoint + length_sppoint) == 0 {
		return make([]common.CompressionIntro, 0)
	}
	sampleratio := float64(length_sppoint / (length_sppoint + length_rdpoint))
	sample_bytes := math.Round(float64(m.M) * sampleratio)
	ready_bytes := float64(m.M) - sample_bytes

	A := make([][]float64, 2)
	for i := 0; i < 2; i++ {
		A[i] = make([]float64, length_rdpoint)
	}
	B := []float64{m.net_bandwitdh, 1}
	C := make([]float64, length_rdpoint)
	D := make([]common.ColumnCmpr, length_rdpoint)
	for i, elem := range m.rdpoint {
		A[0][i] = m.cmpr_cache[elem].CompressionBandwidth
		A[1][i] = 1
		C[i] = -m.cmpr_cache[elem].CompressionGain * m.cmpr_cache[elem].CompressionBandwidth
		D[i] = elem
	}
	if m.flag {
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
		m.alpha = solution.X
		m.flag = false
		m.opt = -solution.Fun
	}
	// fmt.Println("--start once compute, times ", m.test_times, "--")
	x := float64(0)
	for i, elem := range m.alpha {
		x += elem * A[0][i]
	}
	fmt.Println("compute compression bandwitdh", x)
	fmt.Println("real net bandwitdh", m.net_bandwitdh)
	// y := float64(0)
	// noZeroAlpha := 0
	// for _, elem := range alpha {
	// 	y += elem
	// 	if elem > 0 {
	// 		noZeroAlpha++
	// 	}
	// }
	// fmt.Println("alphasum", y)
	// fmt.Println("nozeroalpha", noZeroAlpha)
	// if y < 0.1 {
	for i, elem := range m.alpha {
		if elem > 0 {
			fmt.Println("alpha ", i, " ", elem, " ", A[0][i])
			fmt.Println("Column ", m.rdpoint[i].Column, " ", elem, " CmprType", m.rdpoint[i].Cmpr, " CmprGain", m.cmpr_cache[m.rdpoint[i]].CompressionGain, " CmprBw", m.cmpr_cache[m.rdpoint[i]].CompressionBandwidth)

		}
	}
	// }
	m.test_times += 1
	// fmt.Println("--end once compute--")
	res := make([]common.CompressionIntro, 0, length_rdpoint+length_sppoint)
	for i, elem := range m.rdpoint {
		temp := common.CompressionIntro{}
		// opt为0怎么办?
		a := int64(math.Round(m.alpha[i] * m.cmpr_cache[elem].CompressionBandwidth * m.cmpr_cache[elem].CompressionGain * ready_bytes / m.opt))
		b := newOffAndLen[elem.Column].Len - newOffAndLen[elem.Column].Offset
		if a == 0 || b == 0 {
			continue
		}
		if a > b {
			// res[i].ByteNum = b
			temp.ByteNum = b
			newOffAndLen[elem.Column].Offset += b
		} else {
			// res[i].ByteNum = a
			temp.ByteNum = a
			newOffAndLen[elem.Column].Offset += a
		}
		// res[i].Point = elem
		temp.Point = elem
		res = append(res, temp)

	}
	for _, elem := range m.sppoint {
		temp := common.CompressionIntro{}
		b := newOffAndLen[elem.Column].Len - newOffAndLen[elem.Column].Offset
		a := int64(math.Round(math.Round(sample_bytes / float64(length_sppoint))))
		if a == 0 || b == 0 {
			continue
		}
		if a > b {
			temp.ByteNum = b
			// res[i+length_rdpoint].ByteNum = b
			newOffAndLen[elem.Column].Offset += b
		} else {
			temp.ByteNum = a
			// res[i+length_rdpoint].ByteNum = a
			newOffAndLen[elem.Column].Offset += a
		}
		// res[i+length_rdpoint].Point = elem
		temp.Point = elem
		res = append(res, temp)
	}
	return res
}
