package lightwightcompression

type VarLenCompressor struct {
	datas       [][]byte // all datas.
	dataLen     int      // every data's len in datas.
	lengths     []int
	last        []byte
	firstCalled bool
	length      int
}

// pending 还没决定用[]byte还是int作为入参...
