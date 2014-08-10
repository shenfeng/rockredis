package main

import (
	"testing"
)

func TestSet(t *testing.T) {
	cfg := &RockRedisConf{}

	set(cfg, "addr", "*:6379")
	set(cfg, "databases", "18")
	if cfg.Addr != "*:6379" {
		t.Fail()
	}
	if cfg.Databases != 18 {
		t.Fail()
	}
}

func BenchmarkSet(b *testing.B) {
	cfg := &RockRedisConf{}

	for i := 0; i < b.N; i++ {
		set(cfg, "addr", "*:6379")
		set(cfg, "databases", "18")
	}
}

func TestMemtoll(t *testing.T) {
	if v, err := memtoll("1k"); v != 1024 || err != nil {
		t.Error("failt to convert 1k")
	}

	if v, err := memtoll("10m"); v != 10*1024*1024 || err != nil {
		t.Errorf("failt to convert 10m, get %v", v)
	}

	if v, err := memtoll("10"); v != 10 || err != nil {
		t.Errorf("failt to convert 10, get %v", v)
	}
}
