package compaction

import (
	"pandadb/memtable"
	"pandadb/util"
)

const (
	FileSizeLimit = 2 * 1000 * 1000 * 1000 //2M
)

type DumpWorker struct{}

func NewDumpWorker() *DumpWorker {
	return &DumpWorker{}
}

func (w *DumpWorker) Init() {}

func (w *DumpWorker) Run() {
	for {
		//fmt.Println("worker running...")
		<-memtable.ImRegistry.Wait()
		elem := memtable.ImRegistry.Pop()
		//fmt.Println("pop elem out")
		//fmt.Printf("elem: %v\n", elem)
		util.Assert.NotNil(elem)
		table := elem.GetImTable()
		/*
			如果写commit的时候数据库挂了呢？这时候wal中么有commit记录，但是数据库中已经有了记录。
			这样解决：commit写入使添加version字段，成功写到日志中后将该文件添加到可以成熟文件集合中，成熟的文件意味着该文件可以被读和merge以及提供快照。
			另一方面，如果这时候commit信息没有被写入到wal，那么重启后重新将log文件中的读写内容写入到tab文件，原来的同名文件被覆盖。
		*/
		table.Dump(WorkerP.path)
	}
}

func (w *DumpWorker) Close() {}
