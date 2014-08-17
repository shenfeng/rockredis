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
	now := uint32(time.Now().Unix())
	ks, vs = createKvs(len(values) + 1)

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

func createKvs(size int) (ks, vs [][]byte) {
	kvs := make([][]byte, size*2) // allocate memory just once
	ks, vs = kvs[:size], kvs[size:]
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
	now := uint32(time.Now().Unix())
	ks, vs = createKvs(len(values) + 1)

	size, minseq := li.listMeta()
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
	now := uint32(time.Now().Unix())
	ks, vs = createKvs(len(values) + 1)

	size, minseq := li.listMeta()
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

func (li LinkedList) Pop(a *Arena, mKey []byte, left, right int) (ks, vs [][]byte) {
	llen, minseq := li.listMeta()
	if left+right > llen {
		ks, vs = createKvs(llen + 1)
		ks[0] = mKey
		for i := 0; i < llen; i++ {
			ks[i+1] = listDataKey(a, mKey, minseq+i)
		}
	} else {
		ks, vs = createKvs(left + right + 1)
		now := uint32(time.Now().Unix())
		bigEndian.PutUint32(li, uint32(llen-left-right)) // count
		bigEndian.PutUint32(li[4:], uint32(minseq-left)) // min-seq
		bigEndian.PutUint32(li[12:], now)                // update-ts
		ks[0] = mKey
		vs[0] = []byte(li)

		for i := 0; i < left; i++ {
			ks[i+1] = listDataKey(a, mKey, minseq+i)
		}

		for i := 0; i < right; i++ {
			ks[i+1+left] = listDataKey(a, mKey, minseq+llen-i-1)
		}
	}
	return
}

func (li LinkedList) listMeta() (size, seqstart int) {
	return int(bigEndian.Uint32(li)), int(bigEndian.Uint32(li[4:]))
}
