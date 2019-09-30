package memtable

import (
	"pandadb/log"
	"sync"
)

type Section struct {
	name string
	lock sync.RWMutex
	mem  *MemTable
}

func (s *Section) Set(key, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Wal.WriteKV(s.name, key, value)
	s.mem.Set(key, value)
}

func (s *Section) Get(key string) (string, bool) {
	return s.mem.Get(key)
}

func NewSection(name string) *Section {
	return &Section{name: name, mem:NewMemTable(name)}
}
