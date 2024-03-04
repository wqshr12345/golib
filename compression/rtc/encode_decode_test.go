package rtc_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wqshr12345/golib/compression/rtc"
)

func TestEncoder(t *testing.T) {
	// 将kvPath中的
	// filePath := "C:/南京大学AnyFiles/OneDrive - 南京大学/研二上/阿里Air项目/binlog/binlog.txt"
	filePath := "/home/lluvia/go/src/github.com/go-mysql/binlog4.txt"
	// 使用 ioutil.ReadFile 读取整个文件到字节数组中
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	compressor := rtc.NewRtcCompressor()
	// eventWrapperMetas := make([]common.EventWrapperMeta, 0)

	startTime := time.Now()

	compressedData := compressor.Compress(fileData)

	fmt.Println("文件长度:", len(fileData))
	fmt.Println("压缩后文件长度:", len(compressedData))
	fmt.Println("行转列压缩耗时:", time.Since(startTime))
	fmt.Println("行转列压缩比:", float64(len(compressedData))/float64(len(fileData)))

	decompressor := rtc.NewDecompressor()
	startTime = time.Now()
	decompressedData := decompressor.Decompress(nil, compressedData)
	fmt.Println("行转列压缩耗时:", time.Since(startTime))
	// 验证解压后的数据是否和原始数据一致
	for i := 0; i < len(fileData); i++ {
		if fileData[i] != decompressedData[i] {
			fmt.Println("解压后数据不一致")
			return
		}
	}
}
