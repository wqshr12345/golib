package asyncio

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/wqshr12345/golib/common"
)

type resoruce struct {
	buffer []byte
}

func AsyncCopyBuffer(src io.Reader, dst common.WriteFlusher, bufSize int) (written int64, err error) {
	var wait sync.WaitGroup

	wait.Add(2)
	// Note(wangqian): 根据uber-go最佳实践，Channel Size is One or None，更多的Channel Size除了浪费内存，没有什么其它意义，参考https://juejin.cn/post/7267162307897196578
	bufferChan := make(chan resoruce, 1)
	go writeToChan(&wait, bufferChan, src, bufSize)
	go readFromChan(&wait, bufferChan, dst, &written)
	wait.Wait()
	return written, err
}

func writeToChan(wait *sync.WaitGroup, bufferChan chan (resoruce), src io.Reader, bufSize int) (err error) {
	defer wait.Done()
	for {
		// NOTE(wangqian):Get buffer from pool.
		buffer := make([]byte, bufSize)
		// buffer := common2.GetBuf(bufSize)
		nr, er := src.Read(buffer)
		// read bytes to resource and transport to bufferChan.
		if nr > 0 {
			resource := resoruce{
				buffer: buffer[:nr],
			}
			bufferChan <- resource
		}
		// read to EOF and break the loop.
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	close(bufferChan)
	return err
}
func readFromChan(wait *sync.WaitGroup, bufferChan chan (resoruce), dst common.WriteFlusher, written *int64) {
	defer wait.Done()
	reset := false

	// TODO(wangqian): 暂时写死过期时间——20ms
	ticker := time.NewTicker(time.Duration(20) * time.Millisecond)

Loop:
	for {
		select {
		case resource, ok := <-bufferChan:

			if !ok {
				break Loop
			}
			// reset ticker.
			reset = true
			nw, ew := dst.Write(resource.buffer)

			nr := len(resource.buffer)
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}

			*written += int64(nw)

			if ew != nil {
				//TODO(wangqian):增加错误处理
				// err = ew
				break
			}

			if nr != nw {
				// err = io.ErrShortWrite
				break
			}
		case <-ticker.C:

			if reset {
				reset = false
				continue
			}

			dst.Flush()
		}

	}
	// Note(wangqian): 最后应该Flush一次，避免丢失数据。
	dst.Flush()

}
