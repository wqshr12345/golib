package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	// 指定要处理的目录路径（请根据需要更改目录路径）
	dirPath := "/home/wq/golib/test/all_shuffle/zstd"

	lastNumbersByPrefix := getLatency(dirPath)

	// 打印不同前缀下所有文件的最后一个 latency 数字的统计结果
	for prefix, numbers := range lastNumbersByPrefix {
		fmt.Printf("Prefix: %s\n", prefix)
		fmt.Printf("Numbers: %v\n", numbers)
		sum := 0.0
		for _, number := range numbers {
			sum += number
		}
		fmt.Println("Total Numbers: ", sum)
	}
}

// 统计不同文件前缀下所有文件的最后一个 latency 数字
func getLatency(dirPath string) map[string][]float64 {

	// 定义一个字典来存储每个前缀下的最后一个 latency 数字
	lastNumbersByPrefix := make(map[string][]float64)
	// cmpr := []string{"zstd", "snappy", "lz4", "nocompression", "ours"}
	rateValue := []string{"10*1024*1024", "10*1024*1024", "500*1024*1024", "90*1024*1024", "170*1024*1024", "330*1024*1024", "410*1024*1024", "490*1024*1024", "250*1024*1024"}
	balanceValue := []string{"0*1024*1024", "3*1024*1024", "50*1024*1024", "27*1024*1024", "51*1024*1024", "0*1024*1024", "121*1024*1024", "147*1024*1024", "75*1024*1024", "0*1024*1024"}
	bufferValue := []string{"10*1024*1024", "6*1024*1024*1024", "1*1024*1024", "100*1024*1024", "1*1024*1024*1024"}
	packageValue := []string{"1*1024*1024", "10*1024*1024", "100*1024*1024", "1*1024*1024*1024"}
	prefixes := []string{}
	// 根据上面的组合，生成prefixes
	// for _, c := range cmpr {
	for i, _ := range rateValue {
		for _, buf := range bufferValue {
			for _, pkg := range packageValue {
				prefixes = append(prefixes, fmt.Sprintf("1_%s_%s_%s_%s", buf, pkg, rateValue[i], balanceValue[i]))
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
					lastNumber := extractLastNumber(path)
					if lastNumber != 0 {
						lastNumbersByPrefix[prefix] = append(lastNumbersByPrefix[prefix], lastNumber)
					}
					break
				}
			}
		}

		return nil
	})

	if err != nil {
		fmt.Println("Error walking through directory:", err)
		return lastNumbersByPrefix
	}

	return lastNumbersByPrefix
}

// 从指定路径的文件中提取最后一个包含“latency”行的数字
func extractLastNumber(filePath string) float64 {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", filePath, "-", err)
		return 0
	}
	defer file.Close()

	// 创建一个用于读取文件的扫描器
	scanner := bufio.NewScanner(file)

	// 定义一个变量来保存最后一个包含“latency”行的数字
	var lastNumber float64

	// 遍历文件中的每一行
	for scanner.Scan() {
		line := scanner.Text()

		// 检查行是否以“latency”开头
		if strings.HasPrefix(line, "latency") {
			// 拆分行以获取数字部分
			parts := strings.Fields(line)
			if len(parts) == 2 {
				number, err := strconv.ParseFloat(parts[1], 64)
				if err == nil {
					lastNumber = number
				}
			}
		}
	}

	// 检查是否出现读取错误
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", filePath, "-", err)
		return 0
	}

	// 返回最后一个包含“latency”行的数字
	return lastNumber
}
