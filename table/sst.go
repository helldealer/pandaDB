package table

import (
	"fmt"
	"pandadb/util"
	"sync"
	"sync/atomic"
)

var SstTables *Sst

func Init() {
	//todo: build sst from tables.ind
	SstTables = NewSst()
	fmt.Println("sst init")
}

func NewSst() *Sst {
	return &Sst{
		height: -1,
		layers: make([]Layer, 3, 3),
	}
}

//锁的粒度是否可以更细致
//把计数都改成原子的，别上sst锁
//unmature file 可能用不着，垃圾文件清理时用？
type Sst struct {
	lock          sync.RWMutex //这把锁是性能关键啊，不要随便锁
	/*
	结合layer和大文件的优点：
	第0层由immutable memtable 直接dump生成，数量限制为2000个，单个文件大小为4m左右；
	第1层由merge第0层文件生成，归并时不与第1层已有文件合并，单纯是合并第0层较早文件。因此第1层也存在key重叠的情况，数量为2000，大小为40m。
	第2层也是上一层的合并，文件大小为400m，数量为2000，共800g
	第3层是最后一层，这一层合并上一层的文件，并与本层的文件合并，文件大小4g，数量2000，共计8t，这一层的文件key不重叠，内部有序，外部亦有序
	*/
	layers        []Layer
	height        int32 //current layer num, from 0 to max, default is -1
	matureCount   int32 //file num
	unMatureCount int32
	totalCount    int32
}

func (s *Sst) Merge() {
	s.layers
}

//从tables.ind文件中获取layer和文件名的对应信息，在此之前先获取一共有多少层，保证layers数组不会越界
//没必要加锁，初始化时候顺序写的，如果多线程初始化的话再考虑
//func (s *Sst) insertFileFromFile(name uint64, root string, index []byte, layer int32) {
//	fi := NewFileInfoFromFile(name, root, index)
//	s.layers[layer].matureFiles = append(s.layers[layer].matureFiles, fi)
//}

func (s *Sst) InsertFile(name uint64, root, begin, end string, kvMap map[string]*ValueInfo) {
	if s.height == -1 {
		s.height = 0
	}
	atomic.AddInt32(&s.matureCount, 1)
	fi := NewFileInfoFromCompaction(name, root, begin, end, kvMap)
	layer := s.layers[0]
	layer.lock.Lock()
	defer layer.lock.Unlock()
	if layer.matureFileCount == 2000 {
		//wait major compaction, press the compaction pool be quick!
		panic("not implement wait function")
	}
	if layer.head == layer.tail {
		panic("tail should never catch up head")
	}
	layer.matureFiles[layer.tail] = fi
	layer.matureFileCount++
	layer.tail = (layer.tail + 1) % 2001
}

//将不成熟的转为成熟，要排序
func (s *Sst) matureFile(name string) {

}

func (s *Sst) Get(key string) (string, bool) {
	//原子读
	//todo: 整一个flag标记文件sst非空，这个flag是不需要加锁的，因为一旦sst有内容，就一直会有内容，除非删库
	if s.matureCount == 0 || s.height == -1 {
		return "", false
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	var i int32
	for ; i <= s.height+1; i++ {
		fmt.Println("search in layers")
		layer := s.layers[i]
		if layer.matureFileCount != 0 {
			if layer.head < layer.tail {
				for _, f := range layer.matureFiles[layer.head:layer.tail] {
					//fmt.Println("search in mature files")
					//fmt.Printf("begin:%s, end:%s\n", f.index.begin, f.index.end)
					if f.index.begin <= key && f.index.end >= key {
						//add bloom filter
						vInfo, ok := f.index.kvMap[key]
						if !ok {
							//fmt.Println("skip the no match layer")
							continue
						}
						//fmt.Println("found the result in sst file")
						return string(s.get(f, vInfo.pos, vInfo.len)), true
					}
				}
			}else {
				for _, f := range layer.matureFiles[layer.head:] {
					//fmt.Println("search in mature files")
					//fmt.Printf("begin:%s, end:%s\n", f.index.begin, f.index.end)
					if f.index.begin <= key && f.index.end >= key {
						//add bloom filter
						vInfo, ok := f.index.kvMap[key]
						if !ok {
							//fmt.Println("skip the no match layer")
							continue
						}
						//fmt.Println("found the result in sst file")
						return string(s.get(f, vInfo.pos, vInfo.len)), true
					}
				}
				for _, f := range layer.matureFiles[:layer.tail] {
					//fmt.Println("search in mature files")
					//fmt.Printf("begin:%s, end:%s\n", f.index.begin, f.index.end)
					if f.index.begin <= key && f.index.end >= key {
						//add bloom filter
						vInfo, ok := f.index.kvMap[key]
						if !ok {
							//fmt.Println("skip the no match layer")
							continue
						}
						//fmt.Println("found the result in sst file")
						return string(s.get(f, vInfo.pos, vInfo.len)), true
					}
				}
			}
		}

	}
	//fmt.Println("zero sst height")
	return "", false
}

func (s *Sst) get(f *FileInfo, pos uint32, len uint16) []byte {
	buf := make([]byte, len, len)
	n, err := f.f.ReadAt(buf, int64(pos+2)) //跳过value长度，那么长度是否在file中冗余了
	if err != nil {
		panic("get key from sst failed")
	}
	util.Assert.Equal(n, int(len))
	return buf[:]
}
