package memtable

import (
	"github.com/petar/GoLLRB/llrb"
	"sync"
)

const (
	TableChangeThreshold = 1000
)

const (
	ImPriorityHighest int = iota //最最紧急，系统内部不使用，给api留着
	ImPriorityHigher             //im both tree is not nil
	ImPriorityHigh               //im Tree不为空，bakTree为空
	ImPriorityDefault
	ImPriorityLow
	ImPriorityLower
	ImPriorityLowest
)

//memtable只关心内存就好
type MemTable struct {
	name         string
	lock         sync.RWMutex
	memTree      *llrb.LLRB
	memOnly      bool //:内存db模式还是磁盘模式; :: 不应该放这里，应该往上层移; ::或许放在这里也合适，可以指定某张表存在内存
	immutableMem *ImmutableMemTable
}

type ImmutableMemTable struct {
	lock            sync.RWMutex
	memTree         *llrb.LLRB
	memTreeBak      *llrb.LLRB
	waiteDumpFinish chan struct{} //后台compaction保证这时两棵树都是nil
	elem            *Element      //挂在优先队列里
	version         uint32        //每次init compression， version++; ::should not be here, move it to compact routine
}

func NewImMemTable() *ImmutableMemTable {
	return &ImmutableMemTable{
		waiteDumpFinish: make(chan struct{}),
	}
}

func (m *MemTable) Open() {
	// 从log中恢复memTable，没有的话就新建

}

func (m *MemTable) Close() {
	//：memTable落盘。 ：：落盘应该上层决定
}

func (m *MemTable) Set(k, v string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.memTree.Len() > TableChangeThreshold {
		m.convertToImmutable()
	}
	m.memTree.ReplaceOrInsert(Item{k, v})
}

func (m *MemTable) Get(k string) string {
	m.lock.Lock()
	defer m.lock.Unlock()
	item := Item{k, nil}
	v := m.memTree.Get(item)
	if v == nil {
		//Assert(m.immutableMem != nil)
		im := m.immutableMem
		im.lock.RLock()
		defer im.lock.RUnlock()
		if im.memTreeBak != nil {
			v = im.memTreeBak.Get(item)
			if v != nil {
				return v.(Item).value
			}
		}
		if im.memTree != nil {
			return im.memTree.Get(item).(Item).value
		}
	}
	return v.(Item).value
}

// 外部保证memTable互斥，mem->immutable and registering
func (m *MemTable) convertToImmutable() {
	m.immutableMem.lock.Lock()
	defer m.immutableMem.lock.Unlock()
	if m.immutableMem.memTree != nil {
		//先生成一个临时备用imu table来保证写入不会有较大的百分位点延迟，然后赶紧启动后台compaction
		if m.immutableMem.memTreeBak != nil {
			//只能提升该table的compaction优先级，然后等待后台compaction
			ImRegistry.UpdatePriority(m.immutableMem.elem, ImPriorityHigher)
			m.WaitDumpFinish()
			if m.immutableMem.memTree != nil || m.immutableMem.memTreeBak != nil {
				panic("not clean immutable tree after init compaction")
			}
		} else {
			m.immutableMem.memTreeBak = m.memTree
			if m.immutableMem.elem == nil {
				panic("elem should not be nil when dump to BackTree")
			}
			ImRegistry.UpdatePriority(m.immutableMem.elem, ImPriorityHigh)
			m.memTree = llrb.New()
			return
		}
	}
	m.immutableMem.memTree = m.memTree
	//将immutable注册到compaction memTable优先队列里
	elem := &Element{m.immutableMem, ImPriorityDefault, -1}
	m.immutableMem.elem = elem
	ImRegistry.Push(elem)
	m.memTree = llrb.New()
}

//外部保证immutable互斥
func (m *MemTable) WaitDumpFinish() {
	if m.immutableMem.waiteDumpFinish == nil {
		panic("wait dump finish channel not init")
	}
	<-m.immutableMem.waiteDumpFinish
	m.immutableMem.waiteDumpFinish = make(chan struct{})
}

func NewMemTable() *MemTable {
	return &MemTable{memTree: llrb.New(), memOnly: false, immutableMem: NewImMemTable()}
}

type Item struct {
	key   string
	value string
}

func (a Item) Less(b llrb.Item) bool {
	return a.key < b.(Item).key
}
