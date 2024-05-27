package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	// 输入文件名
	inputFile := "/home/wq/golib/test/all/multiBest/1_10*1024*1024_10*1024*1024_10000*1024*1024_0*1024*1024_100000000_C_1.csv.txt"
	outputFile := "1_10*1024*1024_10*1024*1024_10000*1024*1024_0*1024*1024_100000000_C_1.csv.txt"

	// 打开输入文件
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// 创建输出文件
	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer outFile.Close()

	// 扫描输入文件
	scanner := bufio.NewScanner(file)
	writer := bufio.NewWriter(outFile)

	var writeSection bool

	for scanner.Scan() {
		line := scanner.Text()
		if line == "MultiBest Update Finished." {
			writeSection = true
			writer.WriteString(line + "\n")
		} else if writeSection && strings.HasPrefix(line, "column") {
			writer.WriteString(line + "\n")
		} else if writeSection && !strings.HasPrefix(line, "column") {
			writeSection = false
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	writer.Flush()
}
