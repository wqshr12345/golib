package column_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wqshr12345/golib/compression/column"
	"github.com/wqshr12345/golib/compression/column/common"
	"github.com/wqshr12345/golib/compression/zstd"
)

func TestEncoder(t *testing.T) {
	// 将kvPath中的
	filePath := "C:/南京大学AnyFiles/OneDrive - 南京大学/研二上/阿里Air项目/binlog/binlog.txt"
	// 使用 ioutil.ReadFile 读取整个文件到字节数组中
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	startTime := time.Now()
	zstdCompressor := zstd.NewCompressor()
	zstdCompressedData := zstdCompressor.Compress(fileData)
	fmt.Println("文件长度:", len(fileData))
	fmt.Println("zstd压缩后文件长度:", len(zstdCompressedData))

	midTime := time.Now()
	compressor := column.NewCompressor()
	eventWrapperMetas := make([]common.EventWrapperMeta, 0)
	compressedData := compressor.Compress(fileData, &eventWrapperMetas)
	fmt.Println("文件长度:", len(fileData))
	fmt.Println("压缩后文件长度:", len(compressedData))
	decompressor := column.NewDecompressor()
	decompressedData := decompressor.Decompress(nil, compressedData, eventWrapperMetas, fileData)
	// 验证解压后的数据是否和原始数据一致
	for i := 0; i < len(fileData); i++ {
		if fileData[i] != decompressedData[i] {
			fmt.Println("解压后数据不一致")
			return
		}
	}
	fmt.Println("解压后文件长度:", len(decompressedData))
	fmt.Println("zstd压缩耗时:", midTime.Sub(startTime))
	fmt.Println("行转列压缩解压耗时:", time.Now().Sub(midTime))
}
