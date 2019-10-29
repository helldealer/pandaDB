package table

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"os"
	"pandadb/util"
	"pandadb/version"
	"sync"
)

type Layer struct {
	lock sync.RWMutex
	num  int32 //this layer num
	//can be access by user; 从旧到新排序
	//整成循环队列
	matureFiles       [2001]*FileInfo
	matureFileCount   int16
	tail              int16       //point the next nil place
	head              int16       //point the first element
	unMatureFiles     []*FileInfo //not mature to access
	unmatureFileCount int16

	//compaction fields
}

//把队首的十个文件取出做merge
//1. 从旧的文件开始，将所有key放到一个map里，为啥不放红黑树里，红黑树更新插入较慢。
//2. MAP => rbtree，dump到一个new version文件中
//3. 转为rbtree的过程中维护一张最终涉及到的文件列表和各文件value pos的起始和终止位置。
//4. 归并排序，由于已经在红黑树中排好序了，这个过程会比败者树加速的归并排序还要快。算是10路归并（有可能比10小）吧
func (l *Layer) Merge(root string) (*FileInfo, int16) {
	cache := make(map[string]*ValueInfoWithFile)
	l.lock.RLock()
	tail := l.head + 10
	if l.matureFileCount <= 5 {
		l.lock.RUnlock()
		return nil, 0
	}
	if l.matureFileCount <= 10 {
		tail = l.tail
	}
	//1. big map
	for i, f := range l.matureFiles[l.head:tail] {
		for k, v := range f.index.kvMap {
			cache[k] = &ValueInfoWithFile{v, i}
		}
	}
	l.lock.RUnlock()
	//2. map => llrb
	t := llrb.New()
	for k, v := range cache {
		t.ReplaceOrInsert(&Item{k, v})
	}
	//3. write t to new file
	fi := l.dumpTree(t, root)

	//4. update mature file queue after insert new file in next layer
	return fi, tail - l.head
}

type InputBuffer struct {
	buffer      []byte
	size        uint32
	file        *os.File
	sliceOffset uint32
	fileOffset  uint32
}

func NewInputBuffer(cap, fileOffset uint32, file *os.File) *InputBuffer {
	return &InputBuffer{buffer: make([]byte, cap), size: cap, file: file, fileOffset: fileOffset}
}

func (in *InputBuffer) UpdateInputBuffer(fileOffset uint32) {
	in.fileOffset = fileOffset
	in.sliceOffset = 0
	in.buffer = in.buffer[:0]
}

func (in *InputBuffer) Buffer() []byte {
	return in.buffer
}

func (in *InputBuffer) UpdateOffset(len uint32) bool {
	if len+in.sliceOffset > in.size {
		return false
	}
	in.sliceOffset += len
	in.fileOffset += len
}

type InputBufferPool struct {
	buffers []*InputBuffer
	count   int
}

//lazy init buffer
func NewInputBufferPool(count int) *InputBufferPool {
	return &InputBufferPool{buffers: make([]*InputBuffer, count), count: count}
}

func (in *InputBufferPool) InitBuffer(index, cap, fileOffset uint32, file *os.File) *InputBuffer {
	in.buffers[index] = NewInputBuffer(cap, fileOffset, file)
	return in.buffers[index]
}

func (l *Layer) dumpTree(t *llrb.LLRB, root string) *FileInfo {
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
		kvMap         = make(map[string]*ValueInfo)
		in            *InputBuffer
	)
	index := make([]byte, 3)
	index[0] = 'l'
	index[1] = 'h'
	index[2] = 'r'

	inputPool := NewInputBufferPool(10)

	t.AscendGreaterOrEqual(&Item{}, func(i llrb.Item) bool {
		item := i.(*Item)

		key = item.GetKey()
		if begin == "" {
			begin = key
		}
		keyLen := len(key)
		valueInfo := item.GetValue()
		srcFileIndex := int(l.head) + valueInfo.index
		srcFile := l.matureFiles[srcFileIndex].f
		pos := valueInfo.info.pos
		length := valueInfo.info.len

		//init input buffer
		if inputPool.buffers[srcFileIndex] == nil {
			in = inputPool.InitBuffer(uint32(srcFileIndex), 5e7, valueInfo.info.pos, srcFile)
			n, err := in.file.ReadAt(in.buffer, int64(pos))
			if err != nil {
				panic("read file failed: " + err.Error())
			}
			in.size = uint32(n)
		}

		//find value in buffer, if not exist, reload buffer
		if pos+uint32(length) > in.fileOffset+in.size {
			in.buffer = in.buffer[:0]
			in.sliceOffset = 0
			n, err := in.file.ReadAt(in.buffer, int64(pos))
			if err != nil {
				panic("read file failed: " + err.Error())
			}
			in.size = uint32(n)
			in.fileOffset = pos
		}

		//write value to new file
		offset := pos - in.fileOffset
		_, err = f.Write(in.buffer[offset : offset+uint32(length)+2])
		if err != nil {
			panic("write failed in " + filename)
		}

		index = append(index, util.Uint16ToBigEndBytes(uint16(keyLen))...)
		index = append(index, []byte(key)...)
		index = append(index, util.Uint32ToBigEndBytes(valuePos)...)
		index = append(index, in.buffer[offset:offset+uint32(keyValueWidth)]...)
		kvMap[key] = NewValueInfo(valuePos, uint16(length))
		valuePos += uint32(keyValueWidth + int(length))

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

	//return new file info
	return NewFileInfoFromCompaction(v, root, begin, end, kvMap)
}

//先长打开，后面整个根据访问统计来决定是常打开还是长打开
type FileInfo struct {
	name  uint64
	f     *os.File
	index *FileIndex
}

func newFileInfo(name uint64, root string) *FileInfo {
	fi := &FileInfo{}
	fi.name = name
	filename := fmt.Sprintf("%s/%d.tab", root, name)
	f, err := os.Open(filename)
	if err != nil {
		panic("cannot open the file when build file info")
	}
	fi.f = f
	return fi
}

func NewFileInfoFromCompaction(name uint64, root, begin, end string, kvMap map[string]*ValueInfo) *FileInfo {
	fi := newFileInfo(name, root)
	fi.index = NewFileIndex(begin, end, kvMap)
	return fi
}

func NewFileInfoFromFile(name uint64, root string, index []byte) *FileInfo {
	fi := newFileInfo(name, root)
	fi.index = NewFileIndexFromFile(index)
	return fi
}

type Item struct {
	key   string
	value *ValueInfoWithFile
}

func (i *Item) Less(b llrb.Item) bool {
	return i.key < b.(*Item).key
}

func (i *Item) GetKey() string {
	return i.key
}

func (i *Item) GetValue() *ValueInfoWithFile {
	return i.value
}
