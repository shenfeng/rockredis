package main

import (
	"encoding/binary"
	"testing"
)

func BenchmarkAllocateFromArena(b *testing.B) {
	a := NewArena(1024 * 32)
	for i := 0; i < b.N; i++ {
		buffer := a.Allocate(i%12 + 10)
		binary.BigEndian.PutUint32(buffer, uint32(i)) // count
	}
}

func BenchmarkAllocateFromNew(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, i%12+10)
		binary.BigEndian.PutUint32(buffer, uint32(i)) // count
	}
}
