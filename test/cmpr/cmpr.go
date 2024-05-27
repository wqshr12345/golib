package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	// 指定要处理的目录路径（请根据需要更改目录路径）
	dirPath := "/home/wq/golib/test/incr/ours"

	lastNumbersByPrefix, averageBw, averageGain := getLatency(dirPath)

	// 打印不同前缀下所有文件的最后一个 latency 数字的统计结果
	for prefix, numbers := range lastNumbersByPrefix {
		fmt.Printf("Prefix: %s\n", prefix)
		fmt.Printf("Numbers: %v\n", numbers)
		sum := 0.0
		average := 0.0
		averagegain := 0.0
		times := 0
		for _, number := range numbers {
			sum += number
		}
		for _, number := range averageBw[prefix] {
			average += number
			times++
		}
		for _, number := range averageGain[prefix] {
			averagegain += number
		}
		// fmt.Println("Total Numbers: ", strconv.FormatFloat(sum, 'E', -1, 64))
		fmt.Println("Total CmprLen: ", int(sum))
		fmt.Println("Average BandWidth: ", average/float64(times))
		fmt.Println("Average Gain: ", averagegain/float64(times))
	}
}

// 统计不同文件前缀下所有文件的最后一个 latency 数字
func getLatency(dirPath string) (map[string][]float64, map[string][]float64, map[string][]float64) {

	// 定义一个字典来存储每个前缀下的最后一个 latency 数字
	lastNumbersByPrefix := make(map[string][]float64)
	compressionBws := make(map[string][]float64)
	compressionGains := make(map[string][]float64)
	// cmpr := []string{"zstd", "snappy", "lz4", "nocompression", "ours"}
	rateValue := []string{"10000*1024*1024", "10*1024*1024"}
	// rateValue := []string{"10*1024*1024"}
	// rateValue := []string{"2*1024*1024", "4*1024*1024", "8*1024*1024", "16*1024*1  024", "32*1024*1024", "64*1024*1024", "128*1024*1024", "256*1024*1024", "512*1024*1024", "10*1024*1024", "500*1024*1024", "90*1024*1024", "170*1024*1024", "330*1024*1024", "410*1024*1024", "490*1024*1024", "250*1024*1024", "20*1024*1024", "40*1024*1024", "60*1024*1024", "110*1024*1024", "160*1024*1024", "210*1024*1024", "260*1024*1024", "310*1024*1024", "360*1024*1024", "410*1024*1024", "10*1024*1024"}
	balanceValue := []string{"0*1024*1024", "3*1024*1024", "50*1024*1024", "27*1024*1024", "51*1024*1024", "121*1024*1024", "147*1024*1024", "75*1024*1024", "6*1024*1024", "12*1024*1024"}
	bufferValue := []string{"1048576", "8*1024*1024", "10240", "102400", "10*1024*1024", "2*1024*1024", "1048576", "4*1024*1024", "8*1024*1024", "16*1024*1024", "32*1024*1024", "64*1024*1024", "6*1024*1024*1024", "1*1024*1024", "100*1024*1024", "1*1024*1024*1024", "0*1024*1024"}
	packageValue := []string{"8*1024*1024"}
	// packageValue := []string{"10240", "102400", "1048576", "2*1024*1024", "4*1024*1024", "10*1024*1024", "8*1024*1024", "16*1024*1024", "32*1024*1024", "64*1024*1024"}
	sampleValue := []string{"100000000", "10000000", "10", "20", "30", "40", "50"} // ,"100000000"
	prefixes := []string{}
	// 根据上面的组合，生成prefixes
	// for _, c := range cmpr {
	for _, rate := range rateValue {
		for _, balance := range balanceValue {
			for _, buf := range bufferValue {
				for _, pkg := range packageValue {
					// if dirPath == "/home/wq/golib/test/incr/ours" || dirPath == "/home/wq/golib/test/all/ours" || dirPath == "/home/wq/golib/test/incr/oneBest" || dirPath == "/home/wq/golib/test/incr/multiBest" || dirPath == "/home/wq/golib/test/all/oneBest" || dirPath == "/home/wq/golib/test/all/multiBest" {
					for _, sample := range sampleValue {
						prefixes = append(prefixes, fmt.Sprintf("1_%s_%s_%s_%s_%s", buf, pkg, rate, balance, sample))
					}
					// } else {
					// prefixes = append(prefixes, fmt.Sprintf("1_%s_%s_%s_%s_", buf, pkg, rate, balance))
					// }
				}
			}
		}
	}
	// }
	// 定义文件前缀列表
	// prefixes := []string{"oneBest_1_10*1024*1024_500*1024*1024_0*1024*1024", "oneBest_1_10*1024*1024_10*1024*1024_0*1024*1024", "multiBest_1_10*1024*1024_500*1024*1024_50*1024*1024", "nocompression_1_10*1024*1024_500*1024*1024_50*1024*1024", "lz4_1_10*1024*1024_500*1024*1024_50*1024*1024", "zstd_1_10*1024*1024_500*1024*1024_50*1024*1024", "snappy_1_10*1024*1024_500*1024*1024_50*1024*1024", "lz4_1_10*1024*1024_10*1024*1024_2*1024*1024", "zstd_1_10*1024*1024_10*1024*1024_2*1024*1024", "snappy_1_10*1024*1024_10*1024*1024_2*1024*1024", "multiBest_1_10*1024*1024_10*1024*1024_2*1024*1024", "nocompression_1_10*1024*1024_10*1024*1024_2*1024*1024"} //  "snappy", "multiBest", "nocompression", "oneBest", "zstd"

	// prefixes := []string{"zstd_1_10"}
	// 遍历目录下的所有文件
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("Error accessing file:", path, "-", err)
			return err
		}
		// 检查是否是文件，并且文件名以 ".txt" 结尾
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".txt") {
			// 检查文件前缀
			for _, prefix := range prefixes {
				if strings.HasPrefix(info.Name(), prefix) {
					// 处理文件
					lastNumber, tempBw, tempGain := extractLastNumber(path)
					if lastNumber != 0 {
						lastNumbersByPrefix[prefix] = append(lastNumbersByPrefix[prefix], lastNumber)
					}
					if tempBw != 0 {
						compressionBws[prefix] = append(compressionBws[prefix], tempBw)
						// averageBw += tempBw
						// times++
					}
					if tempGain != 0 {
						compressionGains[prefix] = append(compressionGains[prefix], tempGain)
					}
					break
				}
			}
		}

		return nil
	})

	if err != nil {
		fmt.Println("Error walking through directory:", err)
		return lastNumbersByPrefix, compressionBws, compressionGains
	}

	return lastNumbersByPrefix, compressionBws, compressionGains
}

// 从指定路径的文件中提取最后一个包含“cmprLen”行的数字和"compress bandwitdh:"
func extractLastNumber(filePath string) (float64, float64, float64) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", filePath, "-", err)
		return 0, 0, 0
	}
	defer file.Close()

	// 创建一个用于读取文件的扫描器
	scanner := bufio.NewScanner(file)

	// 定义一个变量来保存包含“cmprLen”行的数字
	var lastNumber float64
	//
	var averageBw float64
	var averageGain float64
	var times int
	var realTimes int
	realTimes = 1
	lastNumber = 0
	// 生成一个0-99的随机数
	random := rand.Intn(10) + 1
	// 遍历文件中的每一行
	// 在下面的for循环中，每100个随机选择一个添加到averageBw中，给我一段代码
	for scanner.Scan() {
		line := scanner.Text()

		// 检查行是否以“latency”开头
		if strings.HasPrefix(line, "cmprLen") {
			// 拆分行以获取数字部分
			parts := strings.Fields(line)
			if len(parts) == 2 {
				number, err := strconv.ParseFloat(parts[1], 64)
				if err == nil {
					lastNumber += number
				}
			}
		}
		if strings.HasPrefix(line, "compress bandwitdh") {
			// 拆分行以获取数字部分
			parts := strings.Fields(line)
			if len(parts) == 3 {
				number, err := strconv.ParseFloat(parts[2], 64)
				if err == nil {
					// 100个随机选择一个添加到averageBw中
					times++
					//
					if times%100 == 0 {
						// 更新random
						random = rand.Intn(10) + 1
					}
					// if times%1 == random {
					averageBw += number
					realTimes++
					// }
				}
			}
		}
		if strings.HasPrefix(line, "compress gain") {
			// 拆分行以获取数字部分
			parts := strings.Fields(line)
			if len(parts) == 3 {
				number, err := strconv.ParseFloat(parts[2], 64)
				if err == nil {
					// 100个随机选择一个添加到averageBw中
					// times++
					//
					if times%100 == random {
						averageGain += number
						// realTimes++
					}
				}
			}
		}
	}
	if realTimes == 0 {
		panic("realTimes is 0")
	}
	averageBw = averageBw / float64(realTimes)
	averageGain = averageGain / float64(realTimes)

	// 检查是否出现读取错误
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", filePath, "-", err)
		return 0, 0, 0
	}

	// 返回最后一个包含“latency”行的数字
	return lastNumber, averageBw, averageGain
}
