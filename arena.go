package main

type Arena struct {
	ram    []byte
	offset int
}

func NewArena(size int) *Arena {
	return &Arena{ram: make([]byte, size)}
}

func (a *Arena) Reset() { a.offset = 0 }

func (a *Arena) Allocate(size int) []byte {
	off := a.offset
	if size < len(a.ram)-off {
		a.offset += size
		return a.ram[off:a.offset]
	} else if size > len(a.ram) {
		return make([]byte, size)
	} else {
		// gc take care of it
		a.ram = make([]byte, len(a.ram))
		a.offset = size
		return a.ram[0:size]
	}
}
