package compaction

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"pandadb/memtable"
)

const (
	FileSizeLimit = 2 * 1000 * 1000 * 1000 //2M
)

//different tables mix in ont file, make multi read fast
//make one go routine first
func Init() {
	wk := NewWorker()
	wk.Init()
	go wk.Run()
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
		memtable.ImRegistry.Wait()
		elem := memtable.ImRegistry.Pop()
		fmt.Println("pop elem out")
		table := elem.GetImTable()
		table.Dump()
		//更新wal日志，把version也写到wal里
	}
}

func (w *Worker) Close() {

}

//这里有个平衡问题：在内存中merge两棵树，dump到一个文件和不合并，dump到两个文件，后续compact时再归并
//现在先选第二种方案
func MergeTrees() *llrb.LLRB {
	panic("need implement")
}

