package adaptive_test

import (
	"fmt"
	"net"
	"os"
	"testing"

	_ "net/http/pprof"

	"github.com/wqshr12345/golib/adaptive"
	"github.com/wqshr12345/golib/expriment"
	"github.com/wqshr12345/golib/limit"
)

// TODO
// 1. 测网络带宽 done
// 2. 加上元数据
// 3. 测延迟...加上时间戳数据... (这个数据不应该算到最终压缩比中，仅在测试中有效) done
func TestAll(t *testing.T) {
	// go func() {
	// 	http.ListenAndServe("0.0.0.0:8081", nil)
	// }()
	// filePath := "/home/lluvia/go/src/github.com/go-mysql/binlog5.txt"
	// filePath := "/home/lluvia/go/src/github.com/go-mysql/binlog.txt"
	// 使用 ioutil.ReadFile 读取整个文件到字节数组中
	fileData, err := os.ReadFile("/home/lluvia/go/src/github.com/go-mysql/binlog0.txt")
	if err != nil {
		panic(err)
	}
	// 网络相关
	// 监听端口
	l, err := net.Listen("tcp", "localhost:12344")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer l.Close()

	// todo 写reader
	go func() {
		conn2, err := net.Dial("tcp", "localhost:12344")
		if err != nil {
			fmt.Println("Error connecting:", err)
			return
		}
		defer conn2.Close()
		reader := expriment.NewMockReader(conn2)

		for {
			reader.Read([]byte{})
		}
	}()
	// var limiter *rate.Limiter
	// limiter := rate.NewLimiter(rate.Limit(float64(1*1024*1024)), int(1*1024*1024))

	// 接受连接
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		return
	}
	defer conn.Close()

	Initer := adaptive.NewIniter(limit.NewRateLimitedWriter(conn, 6*1024*1024*1024, 1*1024*1024*1024), 10*1024*1024*1024, 1)

	// test1——accf
	Initer.Start()

	// test2——zstd
	// Initer.TestByCmprType(common.ZSTD)

	// test3--snappy
	// Initer.TestByCmprType(common.SNAPPY)

	// test4--lz4
	// Initer.TestByCmprType(common.LZ4)

	// test5-OneBest
	// Initer.TestOneBest()

	// test6-MultiBest
	// Initer.TestMultiBest()

	// test7-RtcOneBest
	// Initer.TestRtcOneBest()

	// 阻塞避免程序退出
	Initer.SendBinlogData(fileData)
	// fileData, _ = os.ReadFile("/home/lluvia/go/src/github.com/go-mysql/binlog4.txt")
	// Initer.SendBinlogData(fileData)
	// fileData, _ = os.ReadFile("/home/lluvia/go/src/github.com/go-mysql/binlog3.txt")
	// Initer.SendBinlogData(fileData)
	// fileData, _ = os.ReadFile("/home/lluvia/go/src/github.com/go-mysql/binlog4.txt")
	// Initer.SendBinlogData(fileData)
	// fileData, _ = os.ReadFile("/home/lluvia/go/src/github.com/go-mysql/binlog5.txt")
	// Initer.SendBinlogData(fileData)
	select {}

}
