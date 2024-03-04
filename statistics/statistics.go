package statistics

type TcpStatistic struct {
	RemoteAddr string

	// 用于统计压缩的带宽
}

func (stat *Statistics) FetchCmprTimes(cmprType int) {
	stat.cmprTimes[cmprType] += 1
}
