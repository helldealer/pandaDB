/*
1. 不设置记录sequence，内存中后写入的替换先写入的，memtree和baktree会有重复key
2. 删除不应当是删除红黑树的节点，而是填入一个坟墓
2. dump到文件时，merge memtree和baktree
3. 一次dump，一个文件，暂时不考虑文件大小，只有字段数一个参数限制，后续添加文件大小限制
4. 从log里提供快照？暂时先不解决
5. 大文件号的key较新
*/
package memtable

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"pandadb/util"
	"sync"
	"time"
)

const (
	TableChangeThreshold = 200
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

//-----------------------------------------------------------//

//memtable只关心内存就好
type MemTable struct {
	name            string
	lock            sync.RWMutex
	memTree         *llrb.LLRB
	memOnly         bool //:内存db模式还是磁盘模式; :: 不应该放这里，应该往上层移; ::或许放在这里也合适，可以指定某张表存在内存
	immutableMem    *ImmutableMemTable
	lastCompactTime int64
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
	fmt.Println("set key value")
	fmt.Printf("tree len %d\n", m.memTree.Len())
	if m.memTree.Len() >= TableChangeThreshold {
		fmt.Println("table convert")
		if !m.convertToImmutable() {
			m.WaitDumpFinish()
			m.immutableMem.lock.RLock()
			fmt.Printf("@@@@@@@memtree: %v, bak: %v\n", m.immutableMem.memTree, m.immutableMem.memTreeBak)
			if m.immutableMem.memTree != nil || m.immutableMem.memTreeBak != nil {
				s := fmt.Sprintf("panic tree: %v, bak: %v", m.immutableMem.memTree, m.immutableMem.memTreeBak)
				panic(s)
			}
			m.immutableMem.lock.RUnlock()
			m.convertToImmutable()
		}
		m.lastCompactTime = time.Now().Unix()
	}
	m.memTree.ReplaceOrInsert(&Item{k, v})
}

func (m *MemTable) Get(k string) (string, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	item := &Item{k, ""}
	v := m.memTree.Get(item)
	if v != nil {
		return v.(*Item).value, true
	}

	im := m.immutableMem
	util.Assert.NotNil(im)
	im.lock.RLock()
	defer im.lock.RUnlock()
	if im.memTreeBak != nil {
		v = im.memTreeBak.Get(item)
		if v != nil {
			return v.(*Item).value, true
		}
	}
	if im.memTree != nil {
		v = im.memTree.Get(item)
		if v != nil {
			return v.(*Item).value, true
		}
	}
	return "", false
}

// 外部保证memTable互斥，mem->immutable and registering
func (m *MemTable) convertToImmutable() bool {
	m.immutableMem.lock.Lock()
	defer m.immutableMem.lock.Unlock()
	if m.immutableMem.waitDumpFinish == nil {
		m.immutableMem.SetWait()
	}
	if m.immutableMem.memTree != nil {
		//先生成一个临时备用imu table来保证写入不会有较大的百分位点延迟，然后赶紧启动后台compaction
		if m.immutableMem.memTreeBak != nil {
			//只能提升该table的compaction优先级，然后等待后台compaction
			ImRegistry.UpdatePriority(m.immutableMem.elem, ImPriorityHigher)
			return false
		} else {
			m.immutableMem.memTreeBak = m.memTree
			util.Assert.NotNil(m.immutableMem.elem)
			ImRegistry.UpdatePriority(m.immutableMem.elem, ImPriorityHigh)
			m.memTree = llrb.New()
			return true
		}
	}
	m.immutableMem.memTree = m.memTree
	//将immutable注册到compaction memTable优先队列里
	elem := &Element{m.immutableMem, ImPriorityDefault, -1}
	m.immutableMem.elem = elem
	ImRegistry.Push(elem)
	m.memTree = llrb.New()
	return true
}

//外部保证immutable互斥
func (m *MemTable) WaitDumpFinish() {
	util.Assert.NotNil(m.immutableMem.waitDumpFinish)
	<-m.immutableMem.waitDumpFinish
}

func NewMemTable() *MemTable {
	return &MemTable{memTree: llrb.New(), memOnly: false, immutableMem: NewImMemTable()}
}

//-----------------------------------------------------------//

type Item struct {
	key   string
	value string
}

func (i *Item) Less(b llrb.Item) bool {
	return i.key < b.(*Item).key
}

func (i *Item) GetKey() string {
	return i.key
}

func (i *Item) GetValue() string {
	return i.value
}
