package memtable

import "sync"

var DefaultMaxTables = 1000

var MemPool Pool

type Pool struct {
	lock sync.RWMutex
	count int64
	tables map[string]*MemTable //name -> table
}

func (p *Pool) RegTable(name string, t *MemTable) bool {
	p.lock.RLock()
	if p.tables[name] != nil {
		p.lock.RUnlock()
		return false
	}
	p.lock.RUnlock()
	p.lock.Lock()
	p.tables[name] = t
	p.lock.Unlock()
}

func NewPool() *Pool{
	return &MemPool
}
