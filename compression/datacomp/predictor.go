package datacompadaptive

import (
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wqshr12345/golib/common"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/net"
)

func computeBC(buf []byte) int {
	threshold := len(buf) / 256      // 计算threshold
	byteCounts := make(map[byte]int) // 创建一个map来存储每个字节出现的次数

	count := 0

	for _, b := range buf {
		byteCounts[b]++
		if byteCounts[b] == threshold {
			count++
		}
	}
	return count // 返回满足条件的唯一字节总数
}
func getSysctlValue(key string) (string, error) {
	cmd := exec.Command("sysctl", key)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

type Predictor struct {
	mutex sync.Mutex

	abw     uint64
	cpuLoad float64
	cpuFreq float64

	uload    uint8
	ufreq    uint8
	uabw     uint8
	ubc      uint8
	cmprType int

	table *PredicTable
}

func NewPredictor() *Predictor {
	p := &Predictor{
		mutex: sync.Mutex{},
		table: NewPredicTable(),
	}
	go p.CalAbw()
	return p
}

// 只能在协程中被调用 返回单位是bit/s
func (p *Predictor) CalAbw() {
	lastTxBytes := uint64(0)
	for {
		// link, err := netlink.LinkByName("eth0")
		stat, err := net.IOCounters(true)
		if err != nil {
			panic(err)
		}
		// stat := link.Attrs().Statistics
		tempTxBytes := uint64(0)
		for _, s := range stat {
			tempTxBytes += s.BytesSent
		}

		// 计算ABW
		p.mutex.Lock()
		p.abw = (tempTxBytes - lastTxBytes) * 8
		p.mutex.Unlock()
		fmt.Println("abw: ", p.abw)
		lastTxBytes = tempTxBytes

		// 计算CPU Load
		cpus, _ := cpu.Percent(time.Second, false)
		p.mutex.Lock()
		p.cpuLoad = cpus[0]
		p.mutex.Unlock()
		fmt.Println("cpuLoad: ", p.cpuLoad)

		// 计算CPU Freq
		key := "hw.cpufrequency"
		currentFreqStr, err := getSysctlValue(key)
		if err != nil {
			fmt.Printf("Error fetching current CPU frequency: %s\n", err)
			return
		}

		// 正则表达式来提取速度值
		re := regexp.MustCompile(`\d+`)
		currentFreqValue := re.FindString(currentFreqStr)
		currentFreq, err := strconv.ParseFloat(currentFreqValue, 64)
		if err != nil {
			fmt.Printf("Error parsing current CPU frequency: %s\n", err)
			return
		}
		// 获取最大CPU频率
		key = "hw.cpufrequency_max"
		maxFreqStr, err := getSysctlValue(key)
		if err != nil {
			fmt.Printf("Error fetching max CPU frequency: %s\n", err)
			return
		}

		maxFreqValue := re.FindString(maxFreqStr)
		maxFreq, err := strconv.ParseFloat(maxFreqValue, 64)
		if err != nil {
			fmt.Printf("Error parsing max CPU frequency: %s\n", err)
			return
		}

		// 计算当前CPU频率的百分比
		percentage := (currentFreq / maxFreq) * 100
		// info, _ := cpu.Info()
		// totalFreq := float64(0)
		//
		//	for _, i := range info {
		//		totalFreq += i.Mhz
		//	}
		p.mutex.Lock()
		p.cpuFreq = percentage
		// p.cpuFreq = totalFreq / float64(len(info))
		p.mutex.Unlock()
		fmt.Println("cpuFreq: ", p.cpuFreq)

		time.Sleep(1 * time.Second)
	}
}

func (p *Predictor) GetCpuLoad(cpuload float64) uint8 {
	if cpuload < 34.0 {
		return Cpuload0
	} else if cpuload >= 34.0 && cpuload < 67.0 {
		return Cpuload1
	} else if p.cpuLoad >= 67.0 && cpuload < 100 {
		return Cpuload2
	}
	return Cpuload3
}

func (p *Predictor) GetCpuFreq(cpuFreq float64) uint8 {
	if cpuFreq < 26.0 {
		return Cpufreq0
	} else if cpuFreq >= 26.0 && cpuFreq < 50.0 {
		return Cpufreq1
	} else if cpuFreq >= 50.0 && cpuFreq < 75.0 {
		return Cpufreq2
	} else if cpuFreq >= 75.0 && cpuFreq < 96.0 {
		return Cpufreq3
	}
	return Cpufreq4
}

func (p *Predictor) GetAbw(abw uint64) uint8 {
	if abw < 768*1024 {
		return Abw0
	} else if abw >= 768*1024 && abw < 2*1024*1024 {
		return Abw1
	} else if abw >= 2*1024*1024 && abw < 20*1024*1024 {
		return Abw2
	} else if abw >= 20*1024*1024 && abw < 200*1024*1024 {
		return Abw3
	}
	return Abw4
}

func (p *Predictor) GetBc(bc int) uint8 {
	if bc > 100 {
		return Bc0
	} else if bc >= 67 && bc <= 99 {
		return Bc1
	} else if bc >= 34 && bc <= 66 {
		return Bc2
	}
	return Bc3
}

func (p *Predictor) Predict(buf []byte) int {
	bc := computeBC(buf)
	p.mutex.Lock()
	// 根据论文所说，如果bc>100，那么就不压缩
	if bc > 100 {
		return common.CompressTypeNone
	}
	p.uload = p.GetCpuLoad(p.cpuLoad)
	p.ufreq = p.GetCpuFreq(p.cpuFreq)
	p.uabw = p.GetAbw(p.abw)
	p.ubc = p.GetBc(bc)
	p.mutex.Unlock()

	p.cmprType = p.table.GetCmprType(p.uload, p.ufreq, p.uabw, p.ubc)
	return p.cmprType
}

func (p *Predictor) Update(eor uint32) {
	p.table.UpdateEOR(p.uload, p.ufreq, p.uabw, p.ubc, p.cmprType, eor)
}
