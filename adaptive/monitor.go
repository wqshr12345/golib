package adaptive

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/compression/rtc/lpsimplex"
)

// 用于监控当前的状况，以及设置cache等
type Monitor struct {
	//
	// mlp_info map[byte]byte[]
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

	touseMLP int32
	alpha    []float64

	opt float64
	m   sync.Mutex
}

func NewMonitor(packageSize int, rate float64, epochThreshold int64) *Monitor {
	return &Monitor{
		cmpr_cache:     make(map[common.ColumnCmpr]common.CompressionInfo),
		net_bandwitdh:  rate, // TODOIMP 暂时用固定的...
		timer:          time.NewTimer(5 * time.Second),
		M:              packageSize,
		test_times:     0,
		tempEpoch:      0,
		epochThreshold: epochThreshold,
		flag:           false,
		touseMLP:       0,
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

func (m *Monitor) PrintCacheInfo() {
	fmt.Println("cache info")
	for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
		fmt.Println("cmprType ", cmprType, " cmprGain ", m.cmpr_cache[common.ColumnCmpr{Column: 0x0, Cmpr: byte(cmprType)}].CompressionGain, " cmprBw ", m.cmpr_cache[common.ColumnCmpr{Column: 0x0, Cmpr: byte(cmprType)}].CompressionBandwidth)
	}
}

// 目前的更新策略选择覆盖更新
func (m *Monitor) UpdateCompressionInfo(column byte, cmprtype byte, orilen int, cmprlen int, cmprtime float64, isMb bool) {

	cc := common.ColumnCmpr{Column: column, Cmpr: cmprtype}
	fmt.Println("before update cache: column ", column, " cmprType ", cmprtype, " cmprGain ", m.cmpr_cache[cc].CompressionGain, " cmprBw ", m.cmpr_cache[cc].CompressionBandwidth, "cmprTime", cmprtime)

	// TOOD 临时策略
	if cmprtime == 0 {
		cmprtime = 0.000001
	}
	if (cmprlen == 0) || (cmprtime == 0) {
		panic("cmprlen or cmprtime is zero")
	}
	cmprRatio := float64(orilen) / float64(cmprlen)
	cmprBw := float64(cmprlen) / cmprtime

	ci := common.CompressionInfo{CompressionGain: cmprRatio, CompressionBandwidth: cmprBw, Epoch: m.tempEpoch}
	if isMb || (math.Abs(ci.CompressionBandwidth-m.cmpr_cache[cc].CompressionBandwidth)/m.cmpr_cache[cc].CompressionBandwidth > 0.05) || (math.Abs(ci.CompressionGain-m.cmpr_cache[cc].CompressionGain)/m.cmpr_cache[cc].CompressionGain > 0.05) {
		m.flag = true
		m.cmpr_cache[cc] = ci
		fmt.Println("after update cache: column ", column, " cmprType ", cmprtype, " cmprGain ", m.cmpr_cache[cc].CompressionGain, " cmprBw ", m.cmpr_cache[cc].CompressionBandwidth)
		if m.touseMLP != 2 {
			fmt.Println("touseMLP = 1")
			m.touseMLP = 1
		}
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
				c.Column = byte(i)
				c.Cmpr = byte(j)
				// 针对INT的特定操作
				// if j == common.DELTA || j == common.RLE {
				// 	continue
				// }
				// if (j == common.DELTA || j == common.RLE) && i != common.Int {
				// 	continue
				// }
				// flag := false
				if _, ok := m.cmpr_cache[c]; ok {
					if m.tempEpoch-m.cmpr_cache[c].Epoch > m.epochThreshold {
						samplepoint = append(samplepoint, c)
						// flag = true
					} else {
						readypoint = append(readypoint, c)
					}
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
	// if len(samplepoint) > 0 {
	// 	m.touseMLP = 2
	// }
	if len(readypoint) != len(m.rdpoint) {
		m.flag = true
	}

	m.rdpoint = readypoint
	m.sppoint = samplepoint
}

// 根据当前的cache值以及网络带宽，通过最优化办法计算得到每个列的压缩算法及占比
func (m *Monitor) GetAlphaRatio(offAndLen []common.OffAndLen) []common.CompressionIntro {
	// 打印cache
	// for key, value := range m.cmpr_cache {
	// 	// 	if key.Column == 0x0 {
	// 	fmt.Println("column ", key.Column, " cmprType ", key.Cmpr, " cmprGain ", value.CompressionGain, " cmprBw ", value.CompressionBandwidth)
	// 	// 	}
	// }
	newOffAndLen := make([]common.OffAndLen, len(offAndLen))
	for i, elem := range offAndLen {
		newOffAndLen[i] = elem
	}
	fmt.Println("now epoch", m.tempEpoch)
	m.tempEpoch++
	m.getReadySpPoint(newOffAndLen)
	length_rdpoint := len(m.rdpoint)
	length_sppoint := len(m.sppoint)
	if (length_rdpoint + length_sppoint) == 0 {
		return make([]common.CompressionIntro, 0)
	}
	sampleratio := float64(length_sppoint) / float64(length_sppoint+length_rdpoint)
	sample_bytes := math.Round(float64(m.M) * sampleratio)
	fmt.Println("sample_bytes", sample_bytes)
	ready_bytes := float64(m.M) - sample_bytes
	fmt.Println("ready_bytes", ready_bytes)

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
	// 打印本次应该被采样的点
	fmt.Println("sample point")
	for _, elem := range m.sppoint {
		fmt.Println("Column ", elem.Column, " CmprType", elem.Cmpr)
	}
	// }
	m.test_times += 1
	// fmt.Println("--end once compute--")
	res := make([]common.CompressionIntro, 0, length_rdpoint+length_sppoint)
	for i, elem := range m.rdpoint {
		temp := common.CompressionIntro{}
		// opt为0怎么办?
		fmt.Println("m.opt", m.opt)
		a := int64(math.Round(m.alpha[i] * m.cmpr_cache[elem].CompressionBandwidth * m.cmpr_cache[elem].CompressionGain * ready_bytes / m.opt))
		b := newOffAndLen[elem.Column].Len - newOffAndLen[elem.Column].Offset
		fmt.Println("a", a)
		fmt.Println("b", b)
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
		a := int64(math.Round(math.Round(sample_bytes / float64(length_sppoint))))
		b := newOffAndLen[elem.Column].Len - newOffAndLen[elem.Column].Offset
		fmt.Println("a", a, "b", b)
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
	fmt.Println("cmpr info")

	//修正取整误差
	sum := int(0)
	for i := 0; i < len(res); i++ {
		sum += int(res[i].ByteNum)
	}
	if m.M-sum > 0 && m.M-sum < 100 {
		for i := 0; i < len(res); i++ {
			if res[i].ByteNum > 0 {
				res[i].ByteNum += int64((m.M - sum))
				break
			}
		}
	}
	fmt.Println("firstß")
	for _, elem := range res {
		fmt.Println("Column ", elem.Point.Column, " CmprType", elem.Point.Cmpr, " ByteNum", elem.ByteNum)
	}
	for i := 0; i < len(res); i++ {
		if res[i].ByteNum < int64(float64(m.M)/10) {
			value := res[i].ByteNum
			res[i].ByteNum = 0
			for j := 0; j < len(res); j++ {
				if res[j].ByteNum >= int64(float64(m.M)/10) {
					res[j].ByteNum += value
					break
				}
			}
		}
	}
	var updatedRes []common.CompressionIntro
	for _, r := range res {
		if r.ByteNum > 0 {
			updatedRes = append(updatedRes, r)
		}
	}

	// 更新原始切片
	res = updatedRes
	// sort.Slice(res, func(i, j int) bool {
	// 	// 返回true表示元素i应该在元素j前面，这里用大于实现降序
	// 	return m.cmpr_cache[res[i].Point].CompressionBandwidth > m.cmpr_cache[res[j].Point].CompressionBandwidth
	// })
	sum1 := int(0)
	for i := 0; i < len(res); i++ {
		sum1 += int(res[i].ByteNum)
	}
	for _, elem := range res {
		fmt.Println("Column ", elem.Point.Column, " CmprType", elem.Point.Cmpr, " ByteNum", elem.ByteNum)
	}
	// if sum1 != m.M {
	// 	panic("budeng")
	// }
	return res
}

// func (m *Monitor) UpdateCmprgainofColumns(Points []common.CompressionIntro) {
// 	columns := make(map[byte][]byte)
// 	fmt.Println("testMLP........")
// 	for _, elem := range Points {
// 		columns[elem.Point.Column] = append(columns[elem.Point.Column], elem.Point.Cmpr)
// 		fmt.Println("column", elem.Point.Column, "cmpr", elem.Point.Cmpr)
// 	}
// 	for key, value := range columns {
// 		for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
// 			fmt.Println("cmprType ", cmprType, " cmprGain ", m.cmpr_cache[common.ColumnCmpr{Column: key, Cmpr: byte(cmprType)}].CompressionGain, " cmprBw ", m.cmpr_cache[common.ColumnCmpr{Column: key, Cmpr: byte(cmprType)}].CompressionBandwidth)
// 		}
// 		m.UpdateCmprgainOfOneColumn(value, key)
// 		for cmprType := common.INVALID_START + 1; cmprType < common.INVALID_END; cmprType++ {
// 			fmt.Println("cmprType ", cmprType, " cmprGain ", m.cmpr_cache[common.ColumnCmpr{Column: key, Cmpr: byte(cmprType)}].CompressionGain, " cmprBw ", m.cmpr_cache[common.ColumnCmpr{Column: key, Cmpr: byte(cmprType)}].CompressionBandwidth)
// 		}
// 	}

// }
func (m *Monitor) UpdateMLPgain(Points []common.CompressionIntro) {

	columns := make([]byte, 1)
	for _, elem := range Points {
		f := 0
		for _, i := range columns {
			if elem.Point.Column == i {
				f = 1
				break
			}
		}
		if f == 0 {
			columns = append(columns, elem.Point.Column)
		}
	}
	// 创建一个空的二维切片
	var matrix [][]float64

	// 定义每行的固定内容
	row := make([]float64, common.INVALID_END-common.INVALID_START-2)
	for i := range row {
		row[i] = 1.0
	}
	// 循环n次，每次添加一个row到matrix
	for i := 0; i < len(columns); i++ {
		// 使用append添加一个新的复制的切片
		newRow := make([]float64, len(row))
		copy(newRow, row)
		matrix = append(matrix, newRow)
	}
	for i, columntype := range columns {
		for j := common.INVALID_START + 2; j < common.INVALID_END; j++ {
			c := common.ColumnCmpr{Column: columntype, Cmpr: j}
			matrix[i][j-2-common.INVALID_START] = m.cmpr_cache[c].CompressionGain
		}
	}
	var result strings.Builder
	for _, r := range matrix {
		for j, val := range r {
			result.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			if j < len(r)-1 {
				result.WriteString(",")
			}
		}
		result.WriteString(";")
	}
	// 调用HTTP服务
	url := "http://localhost:5000/traingain" // 确保这是正确的URL
	contentType := "text/plain"              // 设置Content-Type为text/plain，表示发送纯文本格式

	// 使用goroutine发送请求而不等待响应
	go func() {
		_, err := http.Post(url, contentType, bytes.NewBufferString(result.String()))
		if err != nil {
			fmt.Println("Error sending request:", err)
		}
	}()

	// 继续执行其他代码
	// ...
}

func (m *Monitor) UpdateMLPbw(Points []common.CompressionIntro) {

	columns := make([]byte, 1)
	for _, elem := range Points {
		f := 0
		for _, i := range columns {
			if elem.Point.Column == i {
				f = 1
				break
			}
		}
		if f == 0 {
			columns = append(columns, elem.Point.Column)
		}
	}

	// 创建一个空的二维切片
	var matrix [][]float64

	// 定义每行的固定内容
	row := make([]float64, common.INVALID_END-common.INVALID_START-2)
	for i := range row {
		row[i] = 1.0
	}
	// 循环n次，每次添加一个row到matrix
	for i := 0; i < len(columns); i++ {
		// 使用append添加一个新的复制的切片
		newRow := make([]float64, len(row))
		copy(newRow, row)
		matrix = append(matrix, newRow)
	}
	for i, columntype := range columns {
		for j := common.INVALID_START + 2; j < common.INVALID_END; j++ {
			c := common.ColumnCmpr{Column: columntype, Cmpr: j}
			matrix[i][j-2-common.INVALID_START] = m.cmpr_cache[c].CompressionBandwidth
		}
	}
	var result strings.Builder
	for _, r := range matrix {
		for j, val := range r {
			result.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			if j < len(r)-1 {
				result.WriteString(",")
			}
		}
		result.WriteString(";")
	}
	// 调用HTTP服务
	url := "http://localhost:5000/trainbw" // 确保这是正确的URL
	contentType := "text/plain"            // 设置Content-Type为text/plain，表示发送纯文本格式

	// 使用goroutine发送请求而不等待响应
	go func() {
		_, err := http.Post(url, contentType, bytes.NewBufferString(result.String()))
		if err != nil {
			fmt.Println("Error sending request:", err)
		}
	}()

	// 继续执行其他代码
	// ...
}
func (m *Monitor) UpdateCmprgainOfColumns(Points []common.CompressionIntro) {

	columns := make(map[byte][]byte)
	for _, elem := range Points {
		if elem.Point.Cmpr == common.NOCOMPRESSION {
			continue
		}
		columns[elem.Point.Column] = append(columns[elem.Point.Column], elem.Point.Cmpr)
	}
	if len(columns) == 0 {
		return
	}
	// 创建一个空的二维切片
	var matrix [][]float64

	// 定义每行的固定内容
	row := make([]float64, common.INVALID_END-common.INVALID_START-2)
	for i := range row {
		row[i] = 1.0
	}
	// 循环n次，每次添加一个row到matrix
	for i := 0; i < len(columns); i++ {
		// 使用append添加一个新的复制的切片
		newRow := make([]float64, len(row))
		copy(newRow, row)
		matrix = append(matrix, newRow)
	}
	count := 0
	columntypes := make([]byte, 0)
	for columntype, value := range columns {
		columntypes = append(columntypes, columntype)
		for _, elem := range value {
			c := common.ColumnCmpr{Column: columntype, Cmpr: elem}
			matrix[count][elem-2-common.INVALID_START] = m.cmpr_cache[c].CompressionGain
		}
		count += 1
	}
	var result strings.Builder
	for _, r := range matrix {
		for j, val := range r {
			result.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			if j < len(r)-1 {
				result.WriteString(",")
			}
		}
		result.WriteString(";")
	}
	// 调用HTTP服务
	url := "http://localhost:5000/predictgain" // 确保这是正确的URL
	contentType := "text/plain"                // 设置Content-Type为text/plain，表示发送纯文本格式
	// startTime := time.Now()
	resp, err := http.Post(url, contentType, bytes.NewBufferString(result.String()))
	if err != nil {
		fmt.Println("Error sending request:", err)

	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)

	}

	responseString := string(body)
	// duration := time.Since(startTime)
	// fmt.Printf("Execution time: %s\n", duration)
	fmt.Println(responseString)
	newrows := strings.Split(responseString, ";")

	// 初始化一个新的二维切片
	var newmatrix [][]float64

	// 遍历每一行
	for _, newrow := range newrows {
		if newrow == "" {
			continue // 跳过空行（最后一行可能为空）
		}
		// 用逗号分割行来获取单个数值字符串
		values := strings.Split(newrow, ",")
		// 初始化一个新的浮点数切片
		var floatRow []float64
		// 转换字符串为浮点数
		for _, value := range values {
			if value == "" {
				continue // 跳过空值
			}
			floatValue, err := strconv.ParseFloat(value, 64)
			if err != nil {
				fmt.Println("Error parsing float:", err)
				return
			}
			floatRow = append(floatRow, floatValue)
		}
		// 将浮点数行添加到矩阵中
		newmatrix = append(newmatrix, floatRow)
	}
	fmt.Println("inputdata", matrix)
	fmt.Println("outputdata", newmatrix)
	for i, columntype := range columntypes {
		for j := common.INVALID_START + 2; j < common.INVALID_END; j++ {
			if matrix[i][j-2-common.INVALID_START] == 1 {
				c := common.ColumnCmpr{Column: columntype, Cmpr: j}
				temp := m.cmpr_cache[c]
				temp.CompressionGain = newmatrix[i][j-common.INVALID_START-2]
				m.cmpr_cache[c] = temp
			}
		}
	}
}

func (m *Monitor) UpdateCmprbwOfColumns(Points []common.CompressionIntro) {

	columns := make(map[byte][]byte)
	for _, elem := range Points {
		if elem.Point.Cmpr == common.NOCOMPRESSION {
			continue
		}
		columns[elem.Point.Column] = append(columns[elem.Point.Column], elem.Point.Cmpr)
		// fmt.Println("column", elem.Point.Column, "cmpr", elem.Point.Cmpr)
	}
	if len(columns) == 0 {
		return
	}
	// 创建一个空的二维切片
	var matrix [][]float64

	// 定义每行的固定内容
	row := make([]float64, common.INVALID_END-common.INVALID_START-2)
	for i := range row {
		row[i] = 1.0
	}
	// 循环n次，每次添加一个row到matrix
	for i := 0; i < len(columns); i++ {
		// 使用append添加一个新的复制的切片
		newRow := make([]float64, len(row))
		copy(newRow, row)
		matrix = append(matrix, newRow)
	}
	count := 0
	columntypes := make([]byte, 0)
	for columntype, value := range columns {
		columntypes = append(columntypes, columntype)
		for _, elem := range value {
			c := common.ColumnCmpr{Column: columntype, Cmpr: elem}
			matrix[count][elem-2-common.INVALID_START] = m.cmpr_cache[c].CompressionBandwidth
		}
		count += 1
	}
	var result strings.Builder
	for _, r := range matrix {
		for j, val := range r {
			result.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			if j < len(r)-1 {
				result.WriteString(",")
			}
		}
		result.WriteString(";")
	}
	// 调用HTTP服务
	url := "http://localhost:5000/predictbw" // 确保这是正确的URL
	contentType := "text/plain"              // 设置Content-Type为text/plain，表示发送纯文本格式
	// startTime := time.Now()
	temp := result.String()
	if temp == "" {
		fmt.Print("saas")
	}
	resp, err := http.Post(url, contentType, bytes.NewBufferString(result.String()))
	if err != nil {
		fmt.Println("Error sending request:", err)

	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)

	}

	responseString := string(body)
	// duration := time.Since(startTime)
	// fmt.Printf("Execution time: %s\n", duration)
	fmt.Println(responseString)
	newrows := strings.Split(responseString, ";")

	// 初始化一个新的二维切片
	var newmatrix [][]float64

	// 遍历每一行
	for _, newrow := range newrows {
		if newrow == "" {
			continue // 跳过空行（最后一行可能为空）
		}
		// 用逗号分割行来获取单个数值字符串
		values := strings.Split(newrow, ",")
		// 初始化一个新的浮点数切片
		var floatRow []float64
		// 转换字符串为浮点数
		for _, value := range values {
			if value == "" {
				continue // 跳过空值
			}
			floatValue, err := strconv.ParseFloat(value, 64)
			if err != nil {
				fmt.Println("Error parsing float:", err)
				return
			}
			floatRow = append(floatRow, floatValue)
		}
		// 将浮点数行添加到矩阵中
		newmatrix = append(newmatrix, floatRow)
	}
	fmt.Println("inputdata", matrix)
	fmt.Println("outputdata", newmatrix)

	for i, columntype := range columntypes {
		for j := common.INVALID_START + 2; j < common.INVALID_END; j++ {
			if matrix[i][j-2-common.INVALID_START] == 1 {
				c := common.ColumnCmpr{Column: columntype, Cmpr: j}
				temp := m.cmpr_cache[c]
				temp.CompressionBandwidth = newmatrix[i][j-common.INVALID_START-2]
				m.cmpr_cache[c] = temp
			}
		}
	}
}
