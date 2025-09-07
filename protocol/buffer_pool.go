package protocol

import (
	"sync"
)

type BufferPool struct {
	pool sync.Pool
}

type StringsPool struct {
	pool sync.Pool
}

var (
	DefaultBufferPool = NewBufferPool(4096)
	
	DefaultStringsPool = NewStringsPool(16)
)

func NewBufferPool(bufferSize int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, bufferSize)
			},
		},
	}
}

func NewStringsPool(capacity int) *StringsPool {
	return &StringsPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]string, 0, capacity)
			},
		},
	}
}

func (bp *BufferPool) Get() []byte {
	return bp.pool.Get().([]byte)[:0]
}

func (bp *BufferPool) Put(buf []byte) {
	if cap(buf) > 65536 {
		return
	}
	bp.pool.Put(buf[:0])
}

func (sp *StringsPool) Get() []string {
	return sp.pool.Get().([]string)[:0]
}

func (sp *StringsPool) Put(strs []string) {
	if cap(strs) > 1024 {
		return
	}
	
	for i := range strs {
		strs[i] = ""
	}
	
	sp.pool.Put(strs[:0])
}

func GetBuffer() []byte {
	return DefaultBufferPool.Get()
}

func PutBuffer(buf []byte) {
	DefaultBufferPool.Put(buf)
}

func GetStrings() []string {
	return DefaultStringsPool.Get()
}

func PutStrings(strs []string) {
	DefaultStringsPool.Put(strs)
}
