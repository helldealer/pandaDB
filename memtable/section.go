package memtable

import (
	"os"
	"pandadb/log"
	"pandadb/table"
	"sync"
)

type Section struct {
	name  string
	lock  sync.RWMutex
	mem   *MemTable
	wal   *os.File
	sst   *table.Sst
}

type FileInfo struct {
}


func (s *Section) Set(key, value string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Wal.WriteKV(s.name, key, value)
	s.mem.Set(key, value)
}

func (s *Section) Get(key string) (string, bool) {
	v, ok := s.mem.Get(key)
	if ok {
		return v, true
	}
	return s.sst.Get(key)
}

func NewSection(name string) *Section {
	return &Section{name: name, mem: NewMemTable(name)}
}
