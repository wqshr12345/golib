package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/wqshr12345/golib/adaptive"
	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/expriment"
	"github.com/wqshr12345/golib/limit"
)

// 定义一个结构体来存储 "cmprType:" 数据

func getMultiBestType(filename string) [][]common.CmprTypeData {
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("无法打开文件: %s\n", filename)
		return nil
	}
	defer file.Close()

	var data [][]common.CmprTypeData
	var currentMaxTypeData []common.CmprTypeData

	scanner := bufio.NewScanner(file)
	currentMaxTypeFound := false

	// 逐行读取文件
	for scanner.Scan() {
		line := scanner.Text()

		// 检查是否是 "maxType:" 行
		if strings.HasPrefix(line, "maxType:") {
			if currentMaxTypeFound {
				// 将当前的 "maxType:" 数据添加到结果数组中
				data = append(data, currentMaxTypeData)
				// 清空当前的 "maxType:" 数据
				currentMaxTypeData = nil
			}
			// 标记找到新的 "maxType:"
			currentMaxTypeFound = true
		} else if strings.HasPrefix(line, "cmprType:") {
			// 提取 "cmprType:" 数据
			parts := strings.Fields(line)
			if len(parts) >= 4 {
				// 将 "cmprType" 和 "byteNum" 转换为整数类型
				cmprType, err1 := strconv.Atoi(parts[1])
				byteNum, err2 := strconv.Atoi(parts[3])

				if err1 == nil && err2 == nil {
					// 创建 common.CmprTypeData 结构体
					cmprData := common.CmprTypeData{
						CmprType: cmprType,
						ByteNum:  byteNum,
					}

					// 将数据添加到当前 "max_type:" 数据列表中
					currentMaxTypeData = append(currentMaxTypeData, cmprData)
				} else {
					fmt.Printf("解析数据出错: %s\n", line)
				}
			}
		}
	}

	// 将最后一个 "max_type:" 数据添加到结果数组中
	if currentMaxTypeFound {
		data = append(data, currentMaxTypeData)
	}

	// 检查读取文件时是否出错
	if err := scanner.Err(); err != nil {
		fmt.Printf("读取文件时出错: %s\n", filename)
	}

	return data
}

// 遍历文件夹中的所有文件并提取数据
func getMultiBestTypeIncr(filename string) [][]common.CompressionIntro {
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("无法打开文件: %s\n", filename)
		return nil
	}
	defer file.Close()

	var data [][]common.CompressionIntro
	var currentMaxTypeData []common.CompressionIntro

	scanner := bufio.NewScanner(file)
	currentMaxTypeFound := false

	// 逐行读取文件
	for scanner.Scan() {
		line := scanner.Text()

		// 检查是否是 "maxType:" 行
		if strings.HasPrefix(line, "maxType:") {
			if currentMaxTypeFound {
				// 将当前的 "maxType:" 数据添加到结果数组中
				data = append(data, currentMaxTypeData)
				// 清空当前的 "maxType:" 数据
				currentMaxTypeData = nil
			}
			// 标记找到新的 "maxType:"
			currentMaxTypeFound = true
		} else if strings.HasPrefix(line, "column:") {
			// 提取 "column:" 数据
			parts := strings.Fields(line)
			if len(parts) >= 6 {
				// 将 "cmprType" 和 "byteNum" 转换为整数类型
				column, err0 := strconv.Atoi(parts[1])
				cmprType, err1 := strconv.Atoi(parts[3])
				byteNum, err2 := strconv.Atoi(parts[5])

				if err0 == nil && err1 == nil && err2 == nil {
					// 创建 common.CmprTypeData 结构体
					cmprData := common.CompressionIntro{
						Point: common.ColumnCmpr{
							Column: byte(column),
							Cmpr:   byte(cmprType),
						},
						ByteNum: int64(byteNum),
					}

					// 将数据添加到当前 "max_type:" 数据列表中
					currentMaxTypeData = append(currentMaxTypeData, cmprData)
				} else {
					fmt.Printf("解析数据出错: %s\n", line)
				}
			}
		}
	}

	// 将最后一个 "max_type:" 数据添加到结果数组中
	if currentMaxTypeFound {
		data = append(data, currentMaxTypeData)
	}

	// 检查读取文件时是否出错
	if err := scanner.Err(); err != nil {
		fmt.Printf("读取文件时出错: %s\n", filename)
	}

	return data
}

func getOneBestType(dirPath string) []uint8 {

	// 定义一个切片数组来存储所有文件中的数字
	var allNumbers []uint8

	// 遍历指定目录下的所有文件
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("Error accessing file:", path, "-", err)
			return err
		}

		// 检查是否是文件，而不是目录
		if !info.IsDir() {
			// 打开文件
			file, err := os.Open(path)
			if err != nil {
				fmt.Println("Error opening file:", path, "-", err)
				return err
			}
			defer file.Close()

			// 创建一个用于读取文件的扫描器
			scanner := bufio.NewScanner(file)

			// 遍历文件中的每一行
			for scanner.Scan() {
				line := scanner.Text()
				// 将字符串转换为整数
				number, err := strconv.Atoi(line)
				if err != nil {
					fmt.Println("Error converting line to number in file:", path, "-", err)
					continue
				}
				// 将数字添加到数组中
				allNumbers = append(allNumbers, uint8(number))
			}

			// 检查是否出现读取错误
			if err := scanner.Err(); err != nil {
				fmt.Println("Error reading file:", path, "-", err)
			}
		}
		// 继续遍历其他文件
		return nil
	})

	if err != nil {
		fmt.Println("Error walking through directory:", err)
		return allNumbers
	}

	return allNumbers
}

type IntExpression int64

func (i *IntExpression) Set(value string) error {
	parts := strings.Split(value, "*")
	result := int64(1)
	for _, part := range parts {
		num, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid expression: %s", value)
		}
		result *= num
	}
	*i = IntExpression(result)
	return nil
}

func (i *IntExpression) String() string {
	return fmt.Sprintf("%d", *i)
}
func main() {
	var cpuUsage float64
	flag.Float64Var(&cpuUsage, "cpuUsage", 1, "a float64 to set cpu usage")
	var bufferSize IntExpression
	flag.Var(&bufferSize, "bufferSize", "a int to set buffer size")
	var rate IntExpression
	flag.Var(&rate, "rate", "a int to set rate")
	var balance IntExpression
	flag.Var(&balance, "balance", "a int to set balance")
	var packageSize IntExpression
	flag.Var(&packageSize, "packageSize", "a int to set package size")
	var typeName string
	flag.StringVar(&typeName, "typeName", "accf", "a string to set type")
	var fileName string
	flag.StringVar(&fileName, "fileName", "/home/lluvia/go/src/github.com/go-mysql/binlog5.txt", "a string to set binlog name")
	var obName string
	flag.StringVar(&obName, "obName", "/home/lluvia/go/src/github.com/go-mysql/binlog5.txt", "a string to set obBest name")
	var mbName string
	flag.StringVar(&mbName, "mbName", "/home/lluvia/go/src/github.com/go-mysql/binlog5.txt", "a string to set mbBest name")
	var limitThreshold IntExpression
	flag.Var(&limitThreshold, "limitThreshold", "a int to set limit threshold")
	var isFull bool
	flag.BoolVar(&isFull, "isFull", false, "a bool to set isFull")
	flag.Parse()

	// 1. 读取文件内容
	fileData, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}

	totalData := len(fileData)
	// 2. client as a writer.
	l, err := net.Listen("tcp", "localhost:12346")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer l.Close()

	// 3. server as a reader.
	go func() {
		conn2, err := net.Dial("tcp", "localhost:12346")
		if err != nil {
			fmt.Println("Error connecting:", err)
			return
		}
		defer conn2.Close()
		reader := expriment.NewMockReader(conn2, totalData)

		go reader.Panic()
		for {
			reader.Read([]byte{})
		}
	}()

	// 4. client
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		return
	}
	defer conn.Close()

	// read ob mb hyb......
	obBest := getOneBestType(obName)
	// full
	var mbBest [][]common.CmprTypeData
	var hyBest [][]common.CompressionIntro
	if isFull {
		mbBest = getMultiBestType(mbName)
	} else {
		// incr
		hyBest = getMultiBestTypeIncr(mbName)

	}
	// Initer := adaptive.NewIniter(limit.NewRateLimitedWriter(conn, int64(rate), int64(balance), int64(limitThreshold)), int(bufferSize), int(packageSize), cpuUsage, obBest, mbBest, float64(rate), int64(limitThreshold), true)
	Initer := adaptive.NewIniter(limit.NewRateLimiter(conn, float64(rate), 1, int64(limitThreshold)), int(bufferSize), int(packageSize), cpuUsage, obBest, mbBest, hyBest, float64(rate), int64(limitThreshold))

	Initer.SendBinlogData(fileData)

	fmt.Println("typeName: ", typeName)
	fmt.Println("bufferSize: ", bufferSize)
	fmt.Println("rate: ", rate)
	fmt.Println("balance: ", balance)
	fmt.Println("cpuUsage: ", cpuUsage)
	fmt.Println("fileName: ", fileName)
	fmt.Println("obName: ", obName)
	fmt.Println("mbName: ", mbName)
	fmt.Println("packageSize ", packageSize)
	fmt.Println("limitThreshold: ", limitThreshold)
	fmt.Println("isFull: ", isFull)
	fmt.Println("time: ", time.Now())

	if typeName == "ours" {
		Initer.Ours(isFull)
	} else if typeName == "zstd" {
		Initer.TestByCmprType(common.ZSTD, isFull)
	} else if typeName == "snappy" {
		Initer.TestByCmprType(common.SNAPPY, isFull)
	} else if typeName == "lz4" {
		Initer.TestByCmprType(common.LZ4, isFull)
	} else if typeName == "nocompression" {
		Initer.TestByCmprType(common.NOCOMPRESSION, isFull)
	} else if typeName == "oneBest" {
		Initer.TestOneBest(isFull)
	} else if typeName == "multiBest" {
		Initer.TestMultiBest(isFull)
	}

	// 阻塞避免程序退出
	select {}
}
