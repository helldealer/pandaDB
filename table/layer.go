package table

import (
	"fmt"
	"os"
	"pandadb/compaction"
	"sync"
)

type Layer struct {
	lock          sync.RWMutex
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
