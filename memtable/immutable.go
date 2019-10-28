package memtable

import (
	"fmt"
	"os"
	"pandadb/log"
	"pandadb/table"
	"pandadb/util"
	"pandadb/version"
	"sync"

	"github.com/petar/GoLLRB/llrb"
)

//-----------------------------------------------------------//

//两棵树的大小应该不一样，bak树只是一个buffer，应当较小，暂时设置为正常树大小的三分之一，
// mergeTree时将遍历bak树插入memtree
type ImmutableMemTable struct {
	name           string
	sequence       uint64 //memTable convert counter
	lock           sync.RWMutex
	memTree        *llrb.LLRB
	memTreeBak     *llrb.LLRB
	waitDumpFinish chan struct{} //后台compaction保证这时两棵树都是nil
	elem           *Element      //挂在优先队列里
}

//1. 用什么分隔符分割key和value呢，分隔符会被当做key的一部分或value的一部分而被错误的返回
//   先暂时通过编码key的长度在key首解决这个问题，长度先不采用uvarient，直接用两字节的固定宽度，
//   也就是说key和value最长可以为65535字节。
//2. index放在文文件尾，否则需要遍历两遍树

/*
key_value_block: //value_length + value
index header:    //magic number: lhr
index body:      //key_length + key + value_start_pos + value_len: index_length = 2 + key_length + 4 + 2
index tail:      //index_start_pos: 4byte
//length 2bytes
*/
//todo: 这的锁有点糙
func (im *ImmutableMemTable) Dump(root string) {
	im.lock.Lock()
	defer im.lock.Unlock()
	//fmt.Println("start dump!")
	var merged bool
	t := im.memTree
	if im.memTreeBak != nil {
		t = im.MergeTrees()
		merged = true
	}
	im.DumpTree(t, root, merged)
	im.Reset()
}

func (im *ImmutableMemTable) DumpTree(t *llrb.LLRB, root string, merged bool) {
	v := version.VerInfo.IncByOne()
	filename := fmt.Sprintf("%s/%d.tab", root, v)
	//fmt.Println(filename)
	f, err := os.Create(filename)
	if err != nil {
		panic("cannot open file " + filename)
	}
	defer f.Close()

	var (
		//valuePosWidth     = 4
		keyValueWidth = 2
		valuePos      uint32
		key           string
		begin         string
		end           string
		kvMap         = make(map[string]*table.ValueInfo)
	)
	index := make([]byte, 3)
	index[0] = 'l'
	index[1] = 'h'
	index[2] = 'r'

	t.AscendGreaterOrEqual(&Item{}, func(i llrb.Item) bool {
		item := i.(*Item)

		key = item.GetKey()
		if begin == "" {
			begin = key
		}
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
		index = append(index, valueLenBytes...)
		kvMap[key] = table.NewValueInfo(valuePos, uint16(valueLen))
		valuePos += uint32(keyValueWidth + valueLen)

		//fmt.Println("< new entry >--------------:")
		//fmt.Printf("key: %s; key len: %d; key byte: %v\nvaluepos: %d; index: %v\n",
		//	key, keyLen, []byte(key), valuePos, index)
		//fmt.Printf("value: %s; value len: %d, value byte: %v; valuepos: %v\n",
		//	value, valueLen, []byte(value), util.Uint32ToBigEndBytes(valuePos))

		return true
	})
	end = key
	index = append(index, util.Uint32ToBigEndBytes(valuePos)...)

	//fmt.Println("< dump end >------------:")
	//fmt.Printf("file end: index lenth: %d, index %v\n", len(index), index)

	_, err = f.Write(index)
	if err != nil {
		panic("write failed in " + filename)
	}

	//把table file注册到sst的layer0 mature files中
	table.SstTables.InsertFile(v, root, begin, end, kvMap)

	var bak uint64
	if merged {
		//如果merge了两棵树，那么需要在commit信息里把第一棵树的seq信息也加上，
		// 否则wal文件里convert信息和commit信息的seq对应不上
		if im.sequence >= 2 {
			bak = im.sequence - 2
		}
	}
	//因为conver后im的seq自增了，所以这里减1，
	// 如果有baktree的话，由于两次convert对应一次commit，所以要计算bak，把bak也记录进去
	log.Wal.WriteCommit(im.name, im.sequence-1, bak, v) //this should be at the end!
	//info, _ := f.Stat()
	//fmt.Printf("file size: %d\n", info.Size())

}

//外部保证锁
func (im *ImmutableMemTable) Reset() {
	im.memTree = nil
	im.memTreeBak = nil
	im.elem = nil
	close(im.waitDumpFinish)
	im.waitDumpFinish = nil
}

func (im *ImmutableMemTable) SetWait() {
	im.waitDumpFinish = make(chan struct{})
}

//这里有个平衡问题：在内存中merge两棵树，dump到一个文件和不合并，dump到两个文件，后续compact时再归并
//现在先选第二种方案
//当考虑到想sst中注册unmature的file时，第二种方案有很大的不便利性，更换到第一种
func (im *ImmutableMemTable) MergeTrees() *llrb.LLRB {
	im.memTreeBak.AscendGreaterOrEqual(&Item{}, func(i llrb.Item) bool {
		im.memTree.ReplaceOrInsert(i)
		return true
	})
	return im.memTree
}

func NewImMemTable(name string) *ImmutableMemTable {
	return &ImmutableMemTable{
		name:           name,
		waitDumpFinish: make(chan struct{}),
	}
}
