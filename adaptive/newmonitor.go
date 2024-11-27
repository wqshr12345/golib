package adaptive

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/compression/rtc/lpsimplex"
)

// 用于监控当前的状况，以及设置cache等

type MyMap struct {
	dataMap sync.Map
}

func (m *MyMap) Add(key common.ColumnCmpr, value common.CompressionInfo) {
	m.dataMap.Store(key, value)
}

// Get 方法现在如果找不到键，则返回默认值 0
func (m *MyMap) Get(key common.ColumnCmpr) common.CompressionInfo {
	if value, ok := m.dataMap.Load(key); ok {
		// 如果键存在，返回对应的值
		return value.(common.CompressionInfo)
	}
	info := common.CompressionInfo{0, 0, 0}
	return info
}

type Monitor struct {
	//
	// mlp_info map[byte]byte[]
	// key 是列名/数据类型名，value是对应的算法及压缩信息
	// cmpr_cache MyMap
	cmpr_cache map[common.ColumnCmpr]common.CompressionInfo
	// 当前网络带宽 bytes/s
	net_bandwitdh float64
	//传输M需要的时间
	//Bt为行转列的时间
	//
	Bt    float64
	timer *time.Timer
	// 一次压缩大小

	compressunit int
	//方案备选池
	rdpoint []common.ColumnCmpr
	//方案等待备选池(待采样)
	sppoint []common.ColumnCmpr

	test_times int64

	tempEpoch int64

	// 计算cache的阈值
	epochThreshold int64

	touseMLP   int32
	m          sync.Mutex
	CmprThread int

	ReadTime float64

	// 统计信息
	ratioMap map[byte][]float64
	bwMap    map[byte][]float64
}

func NewMonitor(packageSize int, rate float64, epochThreshold int64, cmprThread int, readtime float64) *Monitor {
	return &Monitor{
		// cmpr_cache:     MyMap{},
		cmpr_cache:     make(map[common.ColumnCmpr]common.CompressionInfo),
		net_bandwitdh:  rate,
		timer:          time.NewTimer(5 * time.Second),
		test_times:     0,
		tempEpoch:      0,
		epochThreshold: 100,
		touseMLP:       0,
		compressunit:   100000,
		CmprThread:     cmprThread,
		ReadTime:       readtime,
		ratioMap:       make(map[byte][]float64),
		bwMap:          make(map[byte][]float64),
	}
}

func (m *Monitor) Settransformtime(transformtime float64) {
	m.Bt = transformtime
	fmt.Println("transformtime", transformtime)

}
func (m *Monitor) UpdateBandwidth(bw float64) {
	// if (math.Abs(bw-m.net_bandwitdh) / m.net_bandwitdh) > 0.1 {
	// 	m.flag = true
	// }
	m.net_bandwitdh = bw
	// NOTE(wangqian) :  / float64(m.CmprThread) 不需要这个了!
	// m.net_bandwitdh = 80000000
	// m.Tt = float64(m.M) / m.net_bandwitdh
	fmt.Println("net_bandwitdh", m.net_bandwitdh)
	m.timer.Reset(5 * time.Second)

}

// TODOIMP 并发安全问题
func (m *Monitor) ResetBandWitdh() {
	for range m.timer.C {
		m.net_bandwitdh = math.MaxFloat64
	}
}

func (m *Monitor) PrintCacheInfo() {
	// fmt.Println("cache info")
	// for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
	// 	fmt.Println("cmprType ", cmprType, " cmprGain ", m.cmpr_cache[common.ColumnCmpr{Column: 0x0, Cmpr: byte(cmprType)}].CompressionGain, " cmprBw ", m.cmpr_cache[common.ColumnCmpr{Column: 0x0, Cmpr: byte(cmprType)}].CompressionBandwidth)
	// }
}

// 目前的更新策略选择覆盖更新
func (m *Monitor) UpdateCompressionInfo(column string, cmprtype byte, orilen int, cmprlen int, cmprtime float64, isMb bool) {

	if orilen < m.compressunit {
		return
	}

	cc := common.ColumnCmpr{Column: column, Cmpr: cmprtype}
	// fmt.Println("before update cache: column ", column, " cmprType ", cmprtype, " cmprGain ", m.cmpr_cache[cc].CompressionGain, " cmprBw ", m.cmpr_cache[cc].CompressionBandwidth, "cmprTime", cmprtime)

	// TOOD 临时策略
	if cmprtime == 0 {
		cmprtime = 0.000001
	}
	if (cmprlen == 0) || (cmprtime == 0) {
		panic("cmprlen or cmprtime is zero")
	}
	cmprRatio := float64(orilen) / float64(cmprlen)
	cmprBw := float64(orilen) / cmprtime

	ci := common.CompressionInfo{CompressionGain: cmprRatio, CompressionBandwidth: cmprBw, Epoch: m.tempEpoch}
	if bwVal, ok := m.cmpr_cache[cc]; ok && bwVal.CompressionBandwidth != 0 {
		bwChange := math.Abs(ci.CompressionBandwidth-bwVal.CompressionBandwidth) / ci.CompressionBandwidth
		if bwChange <= 0.2 {
			m.bwMap[cc.Cmpr] = append(m.bwMap[cc.Cmpr], bwChange)
		}
	}

	if gainVal, ok := m.cmpr_cache[cc]; ok && gainVal.CompressionGain != 0 {
		ratioChange := math.Abs(ci.CompressionGain-gainVal.CompressionGain) / ci.CompressionGain
		if ratioChange <= 0.2 {
			m.ratioMap[cc.Cmpr] = append(m.ratioMap[cc.Cmpr], ratioChange)
		}
	}
	if isMb || (math.Abs(ci.CompressionBandwidth-m.cmpr_cache[cc].CompressionBandwidth)/m.cmpr_cache[cc].CompressionBandwidth > 0.05) || (math.Abs(ci.CompressionGain-m.cmpr_cache[cc].CompressionGain)/m.cmpr_cache[cc].CompressionGain > 0.05) {
		// m.flag = true
		m.cmpr_cache[cc] = ci
		// TODO
		// fmt.Println("after update cache: column ", column, " cmprType ", cmprtype, " cmprGain ", m.cmpr_cache[cc].CompressionGain, " cmprBw ", m.cmpr_cache[cc].CompressionBandwidth)
		if m.touseMLP != 2 {
			// fmt.Println("touseMLP = 1")
			m.touseMLP = 1
		}
	}
	// 判断是否需要更新NOCOMPRESSION
	ccTemp := common.ColumnCmpr{Column: column, Cmpr: common.NOCOMPRESSION}

	if _, ok := m.cmpr_cache[ccTemp]; !ok {
		m.cmpr_cache[ccTemp] = common.CompressionInfo{CompressionGain: 1, CompressionBandwidth: 1000000000000}
	}
}

func (m *Monitor) PrintAverageMap() {
	fmt.Println("RatioMap:")
	for key, valueArray := range m.ratioMap {
		sum := 0.0
		for _, value := range valueArray {
			sum += value
		}
		average := sum / float64(len(valueArray))
		fmt.Printf("Key: %s, Average: %f\n", common.GetCompressionTypeStr(key), average)
	}

	// 遍历bwMap并计算平均值打印
	fmt.Println("BwMap:")
	for key, valueArray := range m.bwMap {
		sum := 0.0
		for _, value := range valueArray {
			sum += value
		}
		average := sum / float64(len(valueArray))
		fmt.Printf("Key: %s, Average: %f\n", common.GetCompressionTypeStr(key), average)
	}
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
				c.Column = offAndLen[i].Name
				c.Cmpr = byte(j)
				if m.cmpr_cache[c].CompressionBandwidth != 0 {
					readypoint = append(readypoint, c)

				} else {
					samplepoint = append(samplepoint, c)
				}
			}
		}
	}

	// if len(readypoint) != len(m.rdpoint) {
	// 	m.flag = true
	// }

	m.rdpoint = readypoint
	m.sppoint = samplepoint
}

// 根据当前的cache值以及网络带宽，通过最优化办法计算得到每个列的压缩算法及占比
func (m *Monitor) GetAlphaRatio(offAndLen []common.OffAndLen) map[string][]common.CompressionIntro {
	m.ReadTime = 0.05
	// 打印cache
	// for key, value := range m.cmpr_cache {
	// 	fmt.Println("column ", key.Column, " cmprType ", key.Cmpr, " cmprRatio ", value.CompressionGain, " cmprBw ", value.CompressionBandwidth)
	// }
	// m.cmpr_cache.dataMap.Range(func(key, value interface{}) bool {
	// 	// 这里将interface{}类型转换为实际的类型，根据你的实际数据结构来准确转换
	// 	k := key.(common.ColumnCmpr)
	// 	v := value.(common.CompressionInfo)
	// 	fmt.Println("column ", k.Column, " cmprType ", k.Cmpr, " cmprRatio ", v.CompressionGain, " cmprBw ", v.CompressionBandwidth)
	// 	return true
	// })
	ColumnCounts := make(map[string]int64)
	Result := make(map[string][]common.CompressionIntro)
	ForSequence := make([]string, 0)

	M := int64(0)
	for i, elem := range offAndLen {
		if offAndLen[i].Len-offAndLen[i].Offset > 0 {
			ColumnCounts[elem.Name] = offAndLen[i].Len - offAndLen[i].Offset
			M += (offAndLen[i].Len - offAndLen[i].Offset)
			ForSequence = append(ForSequence, elem.Name)
		}
	}
	// TODO(wangqian): 简单改一下，晚上@guoying 改一下
	if M < 100 {
		return Result
	}
	Tt := float64(M) / m.net_bandwitdh
	sum1 := int64(0)
	for ColumnName := range ColumnCounts {
		sum1 += ColumnCounts[ColumnName]
	}
	//可能会遇到有一列数据在所有包中都没达到最小压缩单位，那就永远更新不了cache？
	flagsample := false
	for _, ColumnName := range ForSequence {
		// 从ZSTD开始，跳过NOCOMPRESSION
		for j := common.INVALID_START + 2; j < common.INVALID_END; j++ {
			elem := common.ColumnCmpr{}
			elem.Column = ColumnName
			elem.Cmpr = byte(j)
			if m.cmpr_cache[elem].CompressionGain == 0 {
				if ColumnCounts[ColumnName] == 0 {
					continue
				}
				flagsample = true
				r := common.CompressionIntro{}
				r.CmprType = j
				r.ByteNum = int64(m.compressunit)
				if ColumnCounts[ColumnName] < r.ByteNum {
					r.ByteNum = ColumnCounts[ColumnName]
				}
				ColumnCounts[ColumnName] -= r.ByteNum
				Result[ColumnName] = append(Result[ColumnName], r)
				// fmt.Println("samplebyteassign", ColumnName, r.CmprType, r.ByteNum)
			}

		}
	}
	if flagsample {
		return Result
	}

	sum0 := int64(0)
	for ColumnName := range ColumnCounts {
		sum0 += ColumnCounts[ColumnName]
	}
	fmt.Println("传输数据原始大小M ", M)

	M = sum0
	Tt = float64(M) / m.net_bandwitdh
	//param
	A := make([][]float64, 1)
	Aeq := make([][]float64, 0)
	B := []float64{-m.Bt}
	Beq := make([]float64, 0)
	C := make([]float64, 0)
	for _, ColumnName := range ForSequence {
		Aeq = append(Aeq, make([]float64, len(ColumnCounts)*int(common.INVALID_END-common.INVALID_START-1)))
		Beq = append(Beq, 1)
		for j := common.INVALID_START + 1; j < common.INVALID_END; j++ {
			elem := common.ColumnCmpr{}
			elem.Column = ColumnName
			elem.Cmpr = byte(j)
			ct := float64(ColumnCounts[ColumnName]) / m.cmpr_cache[elem].CompressionBandwidth
			rT := Tt * (1 / m.cmpr_cache[elem].CompressionGain) * float64(ColumnCounts[ColumnName]) / float64(M)
			A[0] = append(A[0], ct-rT)
			for i := (len(Aeq) - 1) * int(common.INVALID_END-common.INVALID_START-1); i < len(Aeq)*int(common.INVALID_END-common.INVALID_START-1); i++ {
				Aeq[len(Aeq)-1][i] = 1
			}
			C = append(C, rT)
		}
	}

	tol := 1.0e-12
	//tol := float64(0)
	bland := false
	maxiter := 4000
	bounds := make([]lpsimplex.Bound, len(A[0]))
	for i := range bounds {
		bounds[i] = lpsimplex.Bound{0, math.Inf(1)}
	}
	callback := lpsimplex.Callbackfunc(nil)
	disp := false //true

	solution := lpsimplex.LPSimplex(C, A, B, Aeq, Beq, bounds, callback, disp, maxiter, tol, bland)

	// // testc := []float64{-1, 4}

	// // // 定义系数矩阵A
	// // testA := [][]float64{
	// // 	{-3, 1},
	// // 	{1, 2},
	// // }

	// // // 定义等式右边的常数向量b
	// // testb := []float64{6, 4}

	// // // 定义变量x0和x1的边界
	// // x0_bnds := []lpsimplex.Bound{{math.Inf(-1), math.Inf(1)}, {-3, math.Inf(1)}} // 表示没有界限
	// // testsolution := lpsimplex.LPSimplex(testc, testA, testb, nil, nil, x0_bnds, callback, disp, maxiter, tol, bland)
	// // fmt.Printf("testsolution", testsolution.X)
	// // m.flag = false
	opt := solution.Fun
	if opt < m.ReadTime {
		A_ := make([][]float64, 2)
		Aeq_ := make([][]float64, 0)
		B_ := []float64{m.ReadTime, m.ReadTime}
		Beq_ := make([]float64, 0)
		C_ := make([]float64, 0)
		for _, ColumnName := range ForSequence {
			Aeq_ = append(Aeq_, make([]float64, len(ColumnCounts)*int(common.INVALID_END-common.INVALID_START-1)))
			Beq_ = append(Beq_, 1)
			for j := common.INVALID_START + 1; j < common.INVALID_END; j++ {
				elem := common.ColumnCmpr{}
				elem.Column = ColumnName
				elem.Cmpr = byte(j)
				ct := float64(ColumnCounts[ColumnName])/m.cmpr_cache[elem].CompressionBandwidth + m.Bt
				rT := Tt * (1 / m.cmpr_cache[elem].CompressionGain) * float64(ColumnCounts[ColumnName]) / float64(M)
				A_[0] = append(A_[0], rT)
				A_[1] = append(A_[1], ct)
				for i := (len(Aeq_) - 1) * int(common.INVALID_END-common.INVALID_START-1); i < len(Aeq_)*int(common.INVALID_END-common.INVALID_START-1); i++ {
					Aeq_[len(Aeq_)-1][i] = 1
				}
				C_ = append(C_, rT+m.Bt+ct)
			}
		}

		tol := 1.0e-12
		//tol := float64(0)
		bland := false
		maxiter := 4000
		bounds := make([]lpsimplex.Bound, len(A[0]))
		for i := range bounds {
			bounds[i] = lpsimplex.Bound{0, math.Inf(1)}
		}
		callback := lpsimplex.Callbackfunc(nil)
		disp := false //true

		solution = lpsimplex.LPSimplex(C_, A_, B_, Aeq_, Beq_, bounds, callback, disp, maxiter, tol, bland)
		fmt.Println("换一个目标 ")
		opt = solution.Fun

	}
	if math.IsNaN(solution.X[0]) {
		panic("solution.X[0] is NaN")
	}
	alpha := solution.X
	index1 := 0
	sumcompretime := float64(0)
	sumtrantime := float64(0)
	sumcmprlen := float64(0)
	for _, ColumnName := range ForSequence {
		for j := common.INVALID_START + 1; j < common.INVALID_END; j++ {
			elem := common.ColumnCmpr{}
			elem.Column = ColumnName
			elem.Cmpr = byte(j)
			if alpha[index1] > 0 {
				a := alpha[index1] * float64(ColumnCounts[ColumnName]) / m.cmpr_cache[elem].CompressionBandwidth
				b := alpha[index1] * Tt * (1 / m.cmpr_cache[elem].CompressionGain) * float64(ColumnCounts[ColumnName]) / float64(M)
				c := float64(ColumnCounts[ColumnName]) / m.cmpr_cache[elem].CompressionGain
				sumcompretime += a
				sumtrantime += b
				sumcmprlen += c
			}
			if alpha[index1] < 0 {
				fmt.Println("damn ")
			}
			index1 += 1
		}
	}
	// fmt.Println("传输带宽 ", m.net_bandwitdh)
	// fmt.Println("传输时间Tt ", Tt)
	fmt.Println("理论压缩时间 ", sumcompretime)
	fmt.Println("理论传输时间 ", sumtrantime)
	fmt.Println("啦啦传输时间 ", opt)
	// fmt.Println("理论传输数据大小 ", sumcmprlen)
	// fmt.Println("alpha ", m.alpha)
	// for i, elem := range offAndLen {
	// 	fmt.Println(" ", sumcompretime)
	// 	ColumnCounts[elem.Name] = offAndLen[i].Len - offAndLen[i].Offset

	// }

	index := 0
	for _, ColumnName := range ForSequence {
		count := int64(0)
		for j := common.INVALID_START + 1; j < common.INVALID_END; j++ {
			r := common.CompressionIntro{}
			r.CmprType = j
			// fmt.Println("alpha: ", alpha[index], " column: ", ColumnCounts[ColumnName])
			r.ByteNum = int64(math.Floor(alpha[index] * float64(ColumnCounts[ColumnName])))
			count += r.ByteNum
			// if j == common.INVALID_END-1 {
			// r.ByteNum += (ColumnCounts[ColumnName] - count)
			// }
			Result[ColumnName] = append(Result[ColumnName], r)
			// fmt.Println("byteassign", ColumnCounts[ColumnName], r.CmprType, r.ByteNum)
			index++
		}

	}
	for _, ColumnName := range ForSequence {
		count := int64(0)
		for _, elem := range Result[ColumnName] {
			count += elem.ByteNum
		}
		Result[ColumnName][0].ByteNum += (ColumnCounts[ColumnName] - count)

	}
	sum := 0
	for _, ColumnName := range ForSequence {
		for _, elem := range Result[ColumnName] {
			sum += int(elem.ByteNum)
		}
	}
	// fmt.Println("columncountsbefore", sum1)
	// fmt.Println("columncounts", sum0)
	// fmt.Println("assignoverallnum", sum)
	// fmt.Println("M", M)
	m.test_times += 1

	return Result
}
