package table

import (
	"fmt"
	"os"
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

func (s *Sst) InsertFile(name string) {

}

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
	name  string
	f     *os.File
	index *FileIndex
}

type FileIndex struct {
	head   string
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
