package main

import (
	"encoding/binary"
	"time"
)

var (
	bigEndian = binary.BigEndian
)

const (
	SeqStart = 1073741824
)

type LinkedList []byte

func NewLinkedList(a *Arena, key []byte, values [][]byte) (ks, vs [][]byte) {
	kvs := make([][]byte, (len(values)+1)*2) // save memory allocation
	now := uint32(time.Now().Unix())
	ks, vs = kvs[:len(values)+1], kvs[len(values)+1:]

	listMeta := a.Allocate(16)
	bigEndian.PutUint32(listMeta, uint32(len(values)))  // count
	bigEndian.PutUint32(listMeta[4:], uint32(SeqStart)) // min-seq
	bigEndian.PutUint32(listMeta[8:], now)              // add-ts
	bigEndian.PutUint32(listMeta[12:], now)             // update-ts
	ks[0] = key
	vs[0] = listMeta

	for i := 0; i < len(values); i++ {
		ks[i+1] = listDataKey(a, key, SeqStart+i)
		vs[i+1] = values[i]
	}

	return
}

func listDataKey(a *Arena, mKey []byte, seq int) []byte {
	dKey := a.Allocate(len(mKey) + 4 + 1)
	dKey[0] = kListDataKeyPrefix
	dKey[len(mKey)] = ':'
	copy(dKey[1:], mKey[1:]) // ignore the first kListKeyPrefix
	bigEndian.PutUint32(dKey[len(mKey)+1:], uint32(seq))
	return dKey
}

func (li LinkedList) Rpush(a *Arena, key []byte, values [][]byte) (ks, vs [][]byte) {
	kvs := make([][]byte, (len(values)+1)*2) // save memory allocation
	now := uint32(time.Now().Unix())
	ks, vs = kvs[:len(values)+1], kvs[len(values)+1:]

	size, minseq := int(bigEndian.Uint32(li[0:])), int(bigEndian.Uint32(li[4:]))
	bigEndian.PutUint32(li, uint32(len(values)+size)) // count
	bigEndian.PutUint32(li[12:], now)                 // update-ts
	ks[0] = key
	vs[0] = []byte(li)

	for i := 0; i < len(values); i++ {
		ks[i+1] = listDataKey(a, key, minseq+size+i)
		vs[i+1] = values[i]
	}

	return ks, vs
}

func (li LinkedList) Lpush(a *Arena, key []byte, values [][]byte) (ks, vs [][]byte) {
	kvs := make([][]byte, (len(values)+1)*2) // save memory allocation
	now := uint32(time.Now().Unix())
	ks, vs = kvs[:len(values)+1], kvs[len(values)+1:]

	size, minseq := int(bigEndian.Uint32(li[0:])), int(bigEndian.Uint32(li[4:]))
	bigEndian.PutUint32(li, uint32(len(values)+size))       // count
	bigEndian.PutUint32(li[4:], uint32(minseq-len(values))) // min-seq
	bigEndian.PutUint32(li[12:], now)                       // update-ts
	ks[0] = key
	vs[0] = []byte(li)

	for i := 0; i < len(values); i++ {
		ks[i+1] = listDataKey(a, key, minseq-i-1)
		vs[i+1] = values[i]
	}

	return ks, vs
}

func (li LinkedList) listMeta() (size, seqstart int) {
	return int(bigEndian.Uint32(li)), int(bigEndian.Uint32(li[4:]))
}
