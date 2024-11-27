package example

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wqshr12345/golib/adaptive"
	"github.com/wqshr12345/golib/common"
	"github.com/wqshr12345/golib/limit"
)

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
	// read parameter from command line.
	var cmprThread int
	flag.IntVar(&cmprThread, "cmprThread", 1, "a int to set compress thread")

	var generationTime float64
	flag.Float64Var(&generationTime, "generationTime", 0, "The average read time(milliseconds) of each buffer")

	var blockSize float64
	flag.Float64Var(&blockSize, "blockSize", 0.0, "a int to set buffer size")

	var isFull bool
	flag.BoolVar(&isFull, "isFull", false, "a bool to set isFull")

	var rate float64
	flag.Float64Var(&rate, "rate", 0.0, "a int to set rate")

	balance := 0

	var typeName string
	flag.StringVar(&typeName, "typeName", "normal", "a string to set type")

	var fileName string
	flag.StringVar(&fileName, "fileName", "", "a string to set binlog name")

	limitThreshold := 0.0

	epochThreshold := 0.0

	fileData, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	totalData := len(fileData)

	// 2. client as a writer.
	l, err := net.Listen("tcp", "localhost:15123")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer l.Close()

	// 3. server as a reader.
	go func() {
		conn2, err := net.Dial("tcp", "localhost:15123")
		if err != nil {
			fmt.Println("Error connecting:", err)
			return
		}
		defer conn2.Close()
		reader := common.NewMockReader(conn2, totalData)
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

	fmt.Println("typeName: ", typeName)
	fmt.Println("blockSize: ", blockSize)
	fmt.Println("rate: ", rate)
	fmt.Println("cmprThread: ", cmprThread)
	fmt.Println("generationTime: ", generationTime)
	fmt.Println("fileName: ", fileName)
	fmt.Println("isFull: ", isFull)
	fmt.Println("time: ", time.Now())

	generationTime = generationTime * float64(blockSize) / 10.0
	blockSize = blockSize * 1024 * 1024
	packageSize := blockSize
	rate = rate * 1024 * 1024
	balance = balance * 1024 * 1024

	Initer := adaptive.NewIniter(limit.NewRateLimitedWriter(conn, int64(rate), int64(balance), int64(limitThreshold)), int(blockSize), int(packageSize), cmprThread, generationTime, nil, nil, nil, float64(rate), int64(limitThreshold), int64(epochThreshold), true)
	if typeName == "normal" {
		Initer.Run(isFull, rate)
	} else if typeName == "zstd1" {
		Initer.RunByCmprType(common.ZSTD1, isFull, rate)
	} else if typeName == "zstd" {
		Initer.RunByCmprType(common.ZSTD, isFull, rate)
	} else if typeName == "zstd8" {
		Initer.RunByCmprType(common.ZSTD8, isFull, rate)
	} else if typeName == "zstd22" {
		Initer.RunByCmprType(common.ZSTD22, isFull, rate)
	} else if typeName == "snappy" {
		Initer.RunByCmprType(common.SNAPPY, isFull, rate)
	} else if typeName == "lz4" {
		Initer.RunByCmprType(common.LZ4, isFull, rate)
	} else if typeName == "nocompression" {
		Initer.RunByCmprType(common.NOCOMPRESSION, isFull, rate)
	} else if typeName == "lzo" {
		Initer.RunByCmprType(common.LZO, isFull, rate)
	} else if typeName == "gzip" {
		Initer.RunByCmprType(common.GZIP, isFull, rate)
	} else if typeName == "flate" {
		Initer.RunByCmprType(common.FLATE, isFull, rate)
	} else if typeName == "oneBest" {
		Initer.RunOneBest(isFull)
	} else if typeName == "multiBest" {
		Initer.RunMultiBest(isFull)
	}
	Initer.SendBinlogData(fileData)
	select {}
}
