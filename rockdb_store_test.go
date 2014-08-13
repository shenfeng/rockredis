package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestRockdbStore(t *testing.T) {
	path, err := ioutil.TempDir("", "rockredis")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	db, err := NewRockdbStore(path, 0, "")
	if err != nil {
		t.Error(err)
	}

	// k, v := make([]byte, 256), make([]byte, 1024)
	// for i := 0; i < 100000; i++ {
	// 	k[i%len(k)] += 1
	// 	v[i%len(v)] += 1
	// 	// db.Set(k, v)
	// 	// db.Get(k)
	// }
	// fmt.Printf("k size: %d, v size: %d", k, v)

	datas := [][]byte{
		[]byte("key"), []byte("value"), []byte("key2"), []byte("value2"),
	}
	for i := 0; i < len(datas)/2; i += 2 {
		k, v := datas[i], datas[i+1]
		if err = db.Set(k, v); err != nil {
			t.Error(err)
		}

		if r, err := db.Get(k); err != nil {
			t.Error(err)
		} else if !bytes.Equal(r, v) {
			t.Errorf("not get wanted, expect: %v, get: %v", string(v), string(r))
		}

		if err = db.Delete(k); err != nil {
			t.Error(err)
		}

		// should get nil
		if r, err := db.Get(k); err != nil || r != nil {
			t.Error(err)
		}
	}
	db.Close()
}

func BenchmarkRockdbStore(b *testing.B) {
	path, err := ioutil.TempDir("", "rockredis-")
	if err != nil {
		b.Error(err)
	}
	b.Logf("n: %v, using tmp path: %v", b.N, path)
	defer os.RemoveAll(path)
	db, err := NewRockdbStore(path, 0, "snappy")

	// db, err := NewRockdbStore(path, 0, "")
	if err != nil {
		b.Error(err)
	}
	b.ResetTimer()
	k, v := make([]byte, 64), make([]byte, 1024)

	for i := 0; i < b.N; i++ {
		k[i%len(k)] += 1
		v[i%len(v)] += 1
		db.Set(k, v)
		db.Get(k)
	}
	if db != nil {
		db.Close()
	}
}
