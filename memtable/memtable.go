package memtable

import "github.com/petar/GoLLRB/llrb"

//memtable只关心内存就好
type MemTable struct {
	memTree *llrb.LLRB
	memOnly bool //:内存db模式还是磁盘模式; :: 不应该放这里，应该往上层移
}

func (m *MemTable) Open() {
	// 从log中恢复memTable，没有的话就新建

}

func (m *MemTable) Close() {
	//：memTable落盘。 ：：落盘应该上层决定
}

func (m *MemTable) Set(k, v string) {
	
	m.memTree.ReplaceOrInsert(Item{k, v})
}

func (m *MemTable) Get(k string) string {
	v := m.memTree.Get(Item{k, nil})
	if v == nil {
		return ""
	}
	return v.(Item).value
}

func NewMemTable() *MemTable {
	return &MemTable{memTree: llrb.New(), memOnly: false}
}

type Item struct {
	key   string
	value string
}

func (a Item) Less(b llrb.Item) bool {
	return a.key < b.(Item).key
}
