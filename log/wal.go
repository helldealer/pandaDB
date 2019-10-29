package log

import (
	"fmt"
	"os"
	"sync"
)

var (
	Wal                WalLogger
	DefaultBucketNum   uint8 = 6
	DefaultNameFmt           = "%s/%d.wal"
	DefaultKeyValueFmt       = "insert:table:%s,key:%s,value:%v\n"
	//commit 的bak有点词不达意，再改改
	DefaultCommitFmt         = "commit:table:%s,sequence%d,bak%d,version%d\n"
	DefaultConvertFmt        = "convert:table:%s,sequence%d\n"
	DefaultCompaction
	//DefaultK
)

func Init() {
	Wal.bucketNum = DefaultBucketNum
	Wal.buckets = make([]*Bucket, DefaultBucketNum)
	for i, _ := range Wal.buckets {
		b := &Bucket{}
		Wal.buckets[i] = b
		b.name = fmt.Sprintf(DefaultNameFmt, Wal.path, i)
		fmt.Println(b.name)
		var err error
		//todo: not create
		b.file, err = os.Create(b.name)
		if err != nil {
			panic("create wal file failed!")
		}
	}
	fmt.Println("wal start")
}

type WalLogger struct {
	buckets   []*Bucket
	bucketNum uint8
	path      string
}

type Bucket struct {
	name  string
	file  *os.File
	lock  sync.RWMutex
	dirty bool //是否文件结尾不是commit信息
}

func (w *WalLogger) SetPath(p string) {
	w.path = p
}

func (w *WalLogger) WriteKV(table, key, value string) {
	s := fmt.Sprintf(DefaultKeyValueFmt, table, key, value)
	w.Write(table, s, false)
}

func (w *WalLogger) WriteCommit(table string, seq, bak, version uint64) {
	s := fmt.Sprintf(DefaultCommitFmt, table, seq, bak, version)
	w.Write(table, s, true)
}

func (w *WalLogger) WriteTableConvert(table string, seq uint64) {
	s := fmt.Sprintf(DefaultConvertFmt, table, seq)
	w.Write(table, s, false)
}

//todo: all panic is silly!
//写文件要有重试和错误判断，这里是有可能失败的，不能无脑panic
func (w *WalLogger) Write(tableName string, sentence string, commit bool) bool {
	b := w.buckets[TableNameHash(tableName, w.bucketNum)]
	b.lock.Lock()
	defer b.lock.Unlock()
	n, err := b.file.WriteString(sentence)
	if err != nil || n != len(sentence) {
		s := fmt.Sprintf("err:%s,n:%d,len:%d", err, n, len(sentence))
		panic("write failed in file: " + b.name + ": " + s)
	}
	b.dirty = true
	if commit {
		b.dirty = false
	}
	return true
}

/*
recover from wal log:
convert 语句记录了哪个内存表的哪个seq的memtable被convert成immtable并挂到compaction registry了。
commit  语句记录了哪个内存表的哪个seq的imm内存被commit到磁盘了，并记录文件版本号，即文件名
insert 语句记录哪个内存表的插入，更新（就是插入），删除（还是插入。。。）
对于一张指定的内存表，每隔TableChangeThreshold条记录就会有一个convert记录来标志该语句之前的记录属于某个seq的表
软件崩了后：
如果发现某个表没有convert，说明是memTable中的记录丢了
如果发现某个表有convert，但是没有commit，那么说明imuTable中的记录丢了
如果convert丢了且上一个seq的commit也丢了，那么说明两个表都丢了
综述：
wal按照内存表来跟踪丢失与落盘信息
*/
func (w *WalLogger) Recover(){

}

func (w *WalLogger) Close() {
	for _,b := range w.buckets {
		if b.file != nil {
			_ = b.file.Close()
		}
	}
}

//按照表名hash到不同的桶，这里需要找到一种更加均匀的字符串hash算法
func TableNameHash(s string, n uint8) uint8 {
	return uint8(len(s)) % n
}
