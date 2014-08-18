package main

type Arena struct {
	ram    []byte // save memory allocation, which is slow: ~80ns per allocation
	offset int
}

func NewArena(size int) *Arena {
	return &Arena{ram: make([]byte, 0)}
}

// Please make sure all ram allocated from a.ram are free to be reused
func (a *Arena) Reset() { a.offset = 0 }

func (a *Arena) Allocate(size int) []byte {
	off := a.offset
	if size < len(a.ram)-off {
		a.offset += size
		return a.ram[off:a.offset]
	} else if size > len(a.ram) {
		return make([]byte, size) //  allocate directly
	} else { // allocate ram get all used
		a.ram = make([]byte, len(a.ram)) // gc take care of the old a.ram
		a.offset = size
		return a.ram[0:size]
	}
}
