package main

type RamStore map[string][]byte

func NewRamStore() RamStore {
	return make(RamStore)
}

func (s RamStore) Get(key []byte) ([]byte, error) {
	return s[string(key)], nil
}

func (s RamStore) Set(key, val []byte) error {
	s[string(key)] = val
	return nil
}

func (s RamStore) Delete(key []byte) error {
	delete(s, string(key))
	return nil
}

func (s RamStore) Flush() error {
	return nil
}

func (s RamStore) Close() error {
	return nil
}
