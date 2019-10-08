package table

import (
	"fmt"
	"os"
	"pandadb/compaction"
	"pandadb/util"
)

var SstTables Sst

func Init() {
	//build sst from tables.ind
	fmt.Println("sst init")
}

func NewSst() *Sst {
	return &Sst{
	}
}

type Sst struct {
	layers        []Layer
	height        int32 //current layer num
	matureCount   int32 //file num
	unMatureCount int32
	totalCount    int32
}

//从tables.ind文件中获取layer和文件名的对应信息，在此之前先获取一共有多少层，保证layers数组不会越界
func (s *Sst) insertFileFromFile(name uint64, index []byte, layer int32) {
	fi := NewFileInfoFromFile(name, index)
	s.layers[layer].matureFiles = append(s.layers[layer].matureFiles, fi)
}

func (s *Sst) InsertFile(name uint64, begin, end string, kvMap map[string]*ValueInfo) {
	fi := NewFileInfoFromCompaction(name, begin, end, kvMap)
	s.layers[0].unMatureFiles = append(s.layers[0].unMatureFiles, fi)
}

//将不成熟的转为成熟，要排序
func (s *Sst) matureFile(name string) {

}

func (s *Sst) Get(key string) (string, bool) {
	if s.matureCount == 0 || s.height == -1 {
		return "", false
	}
	var i int32
	for ; i <= s.height; i++ {
		for _, f := range s.layers[i].matureFiles {
			if f.index.head <= key && f.index.end >= key {
				//add bloom filter
				vInfo, ok := f.index.kvMap[key]
				if !ok {
					continue
				}
				return string(s.get(f, vInfo.pos, vInfo.len)), true
			}
		}
	}
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

type Layer struct {
	num           int32       //this layer num
	matureFiles   []*FileInfo //can be access by user; 从新到旧排序
	unMatureFiles []*FileInfo //not mature to access
}

//先长打开，后面整个根据访问统计来决定是常打开还是长打开
type FileInfo struct {
	name  uint64
	f     *os.File
	index *FileIndex
}

func newFileInfo(name uint64) *FileInfo {
	fi := &FileInfo{}
	fi.name = name
	filename := fmt.Sprintf("%s/%d.tab", compaction.WorkerP.GetPath(), name)
	f, err := os.Open(filename)
	if err != nil {
		panic("cannot open the file when build file info")
	}
	fi.f = f
	return fi
}

func NewFileInfoFromCompaction(name uint64, begin, end string, kvMap map[string]*ValueInfo) *FileInfo {
	fi := newFileInfo(name)
	fi.index = NewFileIndex(begin, end, kvMap)
	return fi
}

func NewFileInfoFromFile(name uint64, index []byte) *FileInfo {
	fi := newFileInfo(name)
	fi.index = NewFileIndexFromFile(index)
	return fi
}

func NewFileIndex(begin, end string, kvMap map[string]*ValueInfo) *FileIndex {
	fi := &FileIndex{
		begin:begin,
		end:end,
		kvMap: make(map[string]*ValueInfo),
	}
	fi.kvMap = kvMap
	return fi
}

func NewFileIndexFromFile(index []byte) *FileIndex {
	fi := &FileIndex{
		kvMap: make(map[string]*ValueInfo),
	}
	if !(index[0] == 'l' && index[1] == 'h' && index[2] == 'r') {
		panic("magic num is incorrect")
	}
	//todo: 所有的硬编码换成参数
	first := true
	in := index[2:]
	for {
		l := util.BigEndBytesToUint16(in)
		v := &ValueInfo{
			util.BigEndBytesToUint32(in[2+l:]),
			util.BigEndBytesToUint16(in[2+l+4:]),
		}
		k := string(in[2 : l+2])
		fi.kvMap[k] = v
		if first {
			fi.begin = k
			first = false
		}
		in = in[2+l+4+2:]
		if len(in) == 0 {
			fi.end = k
			break
		}
	}
	return fi
}

type FileIndex struct {
	begin  string
	end    string
	kvMap  map[string]*ValueInfo
	filter *BloomFiler
}

type ValueInfo struct {
	pos uint32
	len uint16
}

type BloomFiler struct {
}
