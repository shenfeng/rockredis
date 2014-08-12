package main

import (
	db "github.com/tecbot/gorocksdb"
	"os"
)

type RockdbStore struct {
	ro  *db.ReadOptions
	rro *db.ReadOptions //  do not fill cache
	wo  *db.WriteOptions
	db  *db.DB
}

func NewRockdbStore(path string, cache int, compress string) (*RockdbStore, error) {
	opts := db.NewDefaultOptions()
	opts.SetBlockCache(db.NewLRUCache(cache))
	opts.SetCreateIfMissing(true)
	opts.SetFilterPolicy(db.NewBloomFilter(10))
	opts.SetTargetFileSizeBase(16 * 1024 * 1024) // 16M, default is 2m

	switch compress {
	case "snappy":
		opts.SetCompression(db.SnappyCompression)
	case "zlib":
		opts.SetCompression(db.ZlibCompression)
	case "BZip2":
		opts.SetCompression(db.BZip2Compression)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
	}

	if rockdb, err := db.OpenDb(opts, path); err == nil {
		rro := db.NewDefaultReadOptions()
		rro.SetFillCache(false)
		return &RockdbStore{
			ro:  db.NewDefaultReadOptions(),
			wo:  db.NewDefaultWriteOptions(),
			rro: rro,
			db:  rockdb,
		}, nil
	} else {
		return nil, err
	}
}

func (s *RockdbStore) Get(key []byte) ([]byte, error) {
	if value, err := s.db.Get(s.ro, key); err == nil {
		var d []byte
		d = append(d, value.Data()...) // copy
		value.Free()
		return d, nil
	} else {
		return nil, err
	}
}

func (s *RockdbStore) Set(key, val []byte) error {
	return s.db.Put(s.wo, key, val)
}

func (s *RockdbStore) Delete(key []byte) error {
	return s.db.Delete(s.wo, key)
}

func (s *RockdbStore) Flush() error {
	opts := db.NewDefaultFlushOptions()
	defer opts.Destroy()
	return s.db.Flush(opts)
}

func (s *RockdbStore) Close() error {
	s.db.Close()
	return nil
}

func (s *RockdbStore) Scan(start, end []byte, collector func(key, val []byte) bool) error {
	it := s.db.NewIterator(s.rro)
	defer it.Close()
	it.Seek(start)

	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()

		tmp := make([]byte, 0, key.Size()+value.Size())
		tmp = append(tmp, key.Data()...)
		tmp = append(tmp, value.Data()...)

		if !collector(tmp[:key.Size()], tmp[key.Size():]) {
			key.Free()
			value.Free()
			break
		} else {
			key.Free()
			value.Free()
		}
	}

	return nil
}
