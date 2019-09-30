package memtable

import (
	"sync"
)

var DefaultMaxTables int64 = 1000

var SectionPool Pool

//maybe no need lock, in most situations, app make all tables when it init, never delete tables.
//so there is no write ops on pool when app running
type Pool struct {
	lock   sync.RWMutex
	count  int64
	tables map[string]*Section //name -> table
}

func (p *Pool) RegSection(name string, t *Section) bool {
	p.lock.RLock()
	if p.count >= DefaultMaxTables {
		p.lock.RUnlock()
		return false
	}
	if p.tables[name] != nil {
		p.lock.RUnlock()
		return false
	}
	p.lock.RUnlock()
	p.lock.Lock()
	p.tables[name] = t
	p.count++
	p.lock.Unlock()
	return true
}

func (p *Pool) GetSection(name string) *Section {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.tables[name]
}

func (p *Pool) GetCount() int64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.count
}

func NewPool() *Pool {
	SectionPool.tables = make(map[string]*Section)
	return &SectionPool
}
