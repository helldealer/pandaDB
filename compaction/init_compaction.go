package compaction

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"pandadb/memtable"
	"pandadb/util"
)

const (
	FileSizeLimit = 2 * 1000 * 1000 * 1000 //2M
)

var WorkerP WorkerPool

//different tables mix in ont file, make multi read fast
//make one go routine first
func Init() {
	wk := NewWorker()
	wk.Init()
	go wk.Run()
}

type WorkerPool struct {
	path string
	pool []Worker
}

func (wp *WorkerPool) SetPath(p string) {
	wp.path = p
}

func (wp *WorkerPool) GetPath() string {
	return wp.path
}

func NewWorkerPool() *WorkerPool {
	return &WorkerPool{
	}
}

func (wp *WorkerPool) Close(){

}

type Worker struct {
}

func NewWorker() *Worker {
	return &Worker{}
}

func (w *Worker) Init() {

}

func (w *Worker) Run() {
	for {
		fmt.Println("worker running...")
		<-memtable.ImRegistry.Wait()
		elem := memtable.ImRegistry.Pop()
		fmt.Println("pop elem out")
		fmt.Printf("elem: %v\n", elem)
		util.Assert.NotNil(elem)
		table := elem.GetImTable()
		/*
		如果写commit的时候数据库挂了呢？这时候wal中么有commit记录，但是数据库中已经有了记录。
		这样解决：commit写入使添加version字段，成功写到日志中后将该文件添加到可以成熟文件集合中，成熟的文件意味着该文件可以被读和merge以及提供快照。
		另一方面，如果这时候commit信息没有被写入到wal，那么重启后重新将log文件中的读写内容写入到tab文件，原来的同名文件被覆盖。
		*/
		table.Dump(WorkerP.path, elem.GetSeq(), elem.GetSeqBak())
	}
}

func (w *Worker) Close() {

}

//这里有个平衡问题：在内存中merge两棵树，dump到一个文件和不合并，dump到两个文件，后续compact时再归并
//现在先选第二种方案
//当考虑到想sst中注册unmature的file时，第二种方案有很大的不便利性，更换到第一种
func MergeTrees() *llrb.LLRB {
	panic("need implement")
}
