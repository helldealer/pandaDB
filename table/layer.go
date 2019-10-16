package table

import (
	"fmt"
	"os"
	"sync"
)

type Layer struct {
	lock sync.RWMutex
	num  int32 //this layer num
	//can be access by user; 从旧到新排序
	//整成循环队列
	matureFiles     [2001]*FileInfo
	matureFileCount int16
	tail            int16 //point the next nil place
	head            int16 //point the first element
	unMatureFiles   []*FileInfo //not mature to access
	unmatureFileCount int16

	//compaction fields
}

//把队首的十个文件取出做merge
//1. 从旧的文件开始，将所有key放到一个map里，为啥不放红黑树里，红黑树更新插入较慢。
//2. MAP => rbtree，dump到一个new version文件中
//3. 转为rbtree的过程中维护一张最终涉及到的文件列表和各文件value pos的起始和终止位置。
//4. 归并排序，由于已经在红黑树中排好序了，这个过程会比败者树加速的归并排序还要快。算是10路归并（有可能比10小）吧
func (l *Layer) Merge() *os.FileInfo {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.matureFileCount < 10
	for i, f:= range l.matureFiles[l.head:l.tail] {
		f.index.kvMap[]
	}
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
