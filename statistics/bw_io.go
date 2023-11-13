package statistics

import (
	"io"
	"os"
)

type BandwidthIO struct {
	rwc  io.ReadWriteCloser
	stat bandwidth
}

func (b *BandwidthIO) Read(p []byte) (n int, err error) {
	n, err = b.rwc.Read(p)
	b.stat.AddBandWidth(n)
	return
}

func (b *BandwidthIO) Write(p []byte) (n int, err error) {
	n, err = b.rwc.Write(p)
	return
}

func (b *BandwidthIO) Close() error {
	b.stat.done <- struct{}{}
	return b.rwc.Close()
}

func WithStatistics(rwc io.ReadWriteCloser, output *os.File) io.ReadWriteCloser {
	newBwIO := BandwidthIO{
		rwc: rwc,
		stat: bandwidth{
			done:       make(chan struct{}),
			report:     make(chan string, 10),
			countBytes: 0,
			packages:   0,
			output:     io.Writer(output),
		},
	}
	go newBwIO.stat.printResult()
	return &newBwIO
}
