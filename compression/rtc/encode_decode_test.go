package rtc_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wqshr12345/golib/compression/rtc"
)

// TODO
// 1. 行转列异步化
// 2. 压缩传输异步化
// 3. 乱序发送
// 有一个worker1，专门负责切割数据...然后放到worker1的队列
// 有一个worker2，专门负责行转列...行转列之后就把数据放到worker2的队列
// 有一个worker3，专门负责压缩...乱序压缩每一个buffer，然后发给worker3的队列
// 有一个worker4，专门负责传输...
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

// func TestEncoder2(t *testing.T) {
// 	lp := golp.NewLP(0, 4) // 创建一个线性规划问题，0表示最小化目标函数，4表示变量的数量

// 	// 添加目标函数系数
// 	lp.SetObjFn([]float64{1, 1, 1, 1})

// 	// 添加约束条件
// 	lp.AddConstraint([]float64{10, 4, 5, 7}, golp.LE, 600)
// 	lp.AddConstraint([]float64{2, 2, 6, 10}, golp.LE, 300)
// 	lp.AddConstraint([]float64{8, 5, 2, 2}, golp.LE, 400)

// 	// 求解线性规划问题
// 	lp.Solve()

// 	// 输出结果
// 	fmt.Println("Objective value =", lp.Objective())
// 	fmt.Println("Solution:")
// 	// for i := 1; i <= lp.NumCols(); i++ {
// 	// 	fmt.Printf("x%d = %.2f\n", i, lp.ColPrim(i))
// 	// }
// }
