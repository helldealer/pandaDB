package memtable

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"os"
	"pandadb/log"
	"pandadb/util"
	"pandadb/version"
	"sync"
)

//-----------------------------------------------------------//

type ImmutableMemTable struct {
	name           string
	sequence        uint64 //memTable convert counter
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
index body:      //key_length + key + value_start_pos: index_length = 2 + key_length + 4
index tail:      //index_start_pos: 4byte
//length 2bytes
*/
func (im *ImmutableMemTable) Dump(root string, seq, seqBak uint64) {
	im.lock.Lock()
	defer im.lock.Unlock()
	fmt.Println("start dump!")
	t, bak := im.memTree, im.memTreeBak
	var v uint64
	if t != nil {
		v = im.DumpTree(t, root)
		log.Wal.WriteCommit(im.name, seq, v)
	}
	if bak != nil {
		v= im.DumpTree(bak, root)
		log.Wal.WriteCommit(im.name, seqBak, v)
	}
	fmt.Println("??????##################################")
	im.Reset()
}

func (im *ImmutableMemTable) DumpTree(t *llrb.LLRB, root string) uint64{
	v := version.VerInfo.IncByOne()
	filename := fmt.Sprintf("%s/%d.tab", root, v)
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

		//fmt.Println("< new entry >--------------:")
		//fmt.Printf("key: %s; key len: %d; key byte: %v\nvaluepos: %d; index: %v\n",
		//	key, keyLen, []byte(key), valuePos, index)
		//fmt.Printf("value: %s; value len: %d, value byte: %v; valuepos: %v\n",
		//	value, valueLen, []byte(value), util.Uint32ToBigEndBytes(valuePos))

		return true
	})

	index = append(index, util.Uint32ToBigEndBytes(valuePos)...)

	//fmt.Println("< dump end >------------:")
	//fmt.Printf("file end: index lenth: %d, index %v\n", len(index), index)

	_, err = f.Write(index)
	if err != nil {
		panic("write failed in " + filename)
	}
	//info, _ := f.Stat()
	//fmt.Printf("file size: %d\n", info.Size())
	return v
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

func NewImMemTable(name string) *ImmutableMemTable {
	return &ImmutableMemTable{
		name: name,
		waitDumpFinish: make(chan struct{}),
	}
}
