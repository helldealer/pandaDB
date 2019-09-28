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
	"os"
	"pandadb/util"
	"pandadb/version"
	"sync"
	"time"
)

const (
	TableChangeThreshold = 2
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
			if m.immutableMem.memTree != nil || m.immutableMem.memTreeBak != nil {
				panic("not clean immutable tree after init compaction")
			}
			m.convertToImmutable()
		}
		m.lastCompactTime = time.Now().Unix()
	}
	m.memTree.ReplaceOrInsert(&Item{k, v})
}

func (m *MemTable) Get(k string) string {
	m.lock.Lock()
	defer m.lock.Unlock()
	item := &Item{k, ""}
	v := m.memTree.Get(item)
	if v == nil {
		//Assert(m.immutableMem != nil)
		im := m.immutableMem
		im.lock.RLock()
		defer im.lock.RUnlock()
		if im.memTreeBak != nil {
			v = im.memTreeBak.Get(item)
			if v != nil {
				return v.(*Item).value
			}
		}
		if im.memTree != nil {
			return im.memTree.Get(item).(*Item).value
		}
	}
	return v.(*Item).value
}

// 外部保证memTable互斥，mem->immutable and registering
func (m *MemTable) convertToImmutable() bool {
	m.immutableMem.lock.Lock()
	defer m.immutableMem.lock.Unlock()
	if m.immutableMem.memTree != nil {
		//先生成一个临时备用imu table来保证写入不会有较大的百分位点延迟，然后赶紧启动后台compaction
		if m.immutableMem.memTreeBak != nil {
			//只能提升该table的compaction优先级，然后等待后台compaction
			ImRegistry.UpdatePriority(m.immutableMem.elem, ImPriorityHigher)
			return false
		} else {
			m.immutableMem.memTreeBak = m.memTree
			if m.immutableMem.elem == nil {
				panic("elem should not be nil when dump to BackTree")
			}
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
	if m.immutableMem.waitDumpFinish == nil {
		panic("wait dump finish channel not init")
	}
	<-m.immutableMem.waitDumpFinish
	m.immutableMem.waitDumpFinish = make(chan struct{})
}

func NewMemTable() *MemTable {
	return &MemTable{memTree: llrb.New(), memOnly: false, immutableMem: NewImMemTable()}
}

//-----------------------------------------------------------//

type ImmutableMemTable struct {
	lock           sync.RWMutex
	memTree        *llrb.LLRB
	memTreeBak     *llrb.LLRB
	waitDumpFinish chan struct{} //后台compaction保证这时两棵树都是nil
	elem           *Element      //挂在优先队列里
}

func (im *ImmutableMemTable) GetTrees() (mem, bak *llrb.LLRB) {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.memTree, im.memTreeBak
}

//1. 用什么分隔符分割key和value呢，分隔符会被当做key的一部分或value的一部分而被错误的返回
//   先暂时通过编码key的长度在key首解决这个问题，长度先不采用uvarient，直接用两字节的固定宽度，
//   也就是说key和value最长可以为65535字节。
//2. index放在文文件尾，否则需要遍历两遍树

/*
key_value_block: //value_length + value
index header:    //magic number: lhr
index body:      //key_length + key + value_start_pos: index_length = 2 + key_length + 4
index tail:      //index_start_pos: 4byte
//length 2bytes
*/
func (im *ImmutableMemTable) Dump() {
	im.lock.Lock()
	defer im.lock.Unlock()
	fmt.Println("start dump!")
	t, bak := im.memTree, im.memTreeBak
	if t != nil {
		im.DumpTree(t)
	}
	if bak != nil {
		im.DumpTree(bak)
	}
	im.memTree = nil
	im.memTreeBak = nil
	close(im.waitDumpFinish)
}

func (im *ImmutableMemTable) DumpTree(t *llrb.LLRB) {
	v := version.VerInfo.IncByOne()
	filename := fmt.Sprintf("./tmp/%d.tab", v)
	fmt.Println(filename)
	f, err := os.Create(filename)
	if err != nil {
		panic("cannot open file " + filename)
	}
	defer f.Close()

	var (
		//valuePosWidth     = 4
		keyValueWidth = 2
		valuePos      uint32
	)
	index := make([]byte, 3)
	index[0] = 'l'
	index[1] = 'h'
	index[2] = 'r'

	t.AscendGreaterOrEqual(&Item{}, func(i llrb.Item) bool {
		item := i.(*Item)

		key := item.GetKey()
		keyLen := len(key)

		value := item.GetValue()
		valueLen := len(value)
		valueLenBytes := util.Uint16ToBigEndBytes(uint16(valueLen))
		bodyEntry := append(valueLenBytes, []byte(value)...)
		_, err := f.Write(bodyEntry) //返回的写入数量 暂时不检查，等到把统计文件大小也当做dump的参数时再考虑
		if err != nil {
			panic("write failed in " + filename)
		}

		index = append(index, util.Uint16ToBigEndBytes(uint16(keyLen))...)
		index = append(index, []byte(key)...)
		index = append(index, util.Uint32ToBigEndBytes(valuePos)...)
		valuePos += uint32(keyValueWidth + valueLen)

		fmt.Println("< new entry >--------------:")
		fmt.Printf("key: %s; key len: %d; key byte: %v\nvaluepos: %d; index: %v\n",
			key, keyLen, []byte(key), valuePos, index)
		fmt.Printf("value: %s; value len: %d, value byte: %v; valuepos: %v\n",
			value, valueLen, []byte(value), util.Uint32ToBigEndBytes(valuePos))

		return true
	})

	index = append(index, util.Uint32ToBigEndBytes(valuePos)...)

	fmt.Println("< dump end >------------:")
	fmt.Printf("file end: index lenth: %d, index %v\n", len(index), index)

	_, err = f.Write(index)
	if err != nil {
		panic("write failed in " + filename)
	}
	info, _ := f.Stat()
	fmt.Printf("file size: %d\n", info.Size())
}

func (im *ImmutableMemTable) Reset() {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.memTree = nil
	im.memTreeBak = nil
	im.elem = nil
	close(im.waitDumpFinish)
}

func NewImMemTable() *ImmutableMemTable {
	return &ImmutableMemTable{
		waitDumpFinish: make(chan struct{}),
	}
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
