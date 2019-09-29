package log

import (
	"fmt"
	"os"
	"sync"
)


var (
	Wal WalLogger
	DefaultBucketNum uint8 = 6
	DefaultNameFmt = "%d.wal"
	//DefaultK
)

func init(){
	Wal.bucketNum = DefaultBucketNum
	Wal.buckets = make([]Bucket, DefaultBucketNum)
	for i, b := range Wal.buckets {
		b.name = fmt.Sprintf(DefaultNameFmt, i)
		var err error
		b.file, err = os.Create(b.name)
		if err != nil {
			panic("create wal file failed!")
		}
	}
	fmt.Println("wal start")
}

type WalLogger struct {
	buckets   []Bucket
	bucketNum uint8
}

type Bucket struct {
	name  string
	file  *os.File
	lock  sync.RWMutex
	dirty bool //是否文件结尾不是commit信息
}

//todo: all panic is silly!
func (w *WalLogger) Write(tableName string, sentence string) bool {
	b := w.buckets[TableNameHash(tableName, w.bucketNum)]
	b.lock.Lock()
	defer b.lock.Unlock()
	n, err := b.file.WriteString(sentence)
	if err != nil || n != len(sentence) {
		panic("write failed in file: " + b.name)
	}
	return true
}

//按照表名hash到不同的桶，这里需要找到一种更加均匀的字符串hash算法
func TableNameHash(s string, n uint8) uint8 {
	return uint8(len(s)) % n
}
