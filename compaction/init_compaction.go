package compaction

import "pandadb/memtable"

const (
	FileSizeLimit = 2 * 1000 * 1000 * 1000 //2M
)

//different tables mix in ont file, make multi read fast
//make one go routine first
func init() {
	wk := NewWorker()
	wk.Init()
	go wk.Run()
}

type Info struct {
	version uint64 //same as fileNum, version++ when a new file create
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
		memtable.ImRegistry.Wait()
		elem := memtable.ImRegistry.Pop()
		elem
	}

}

func (w *Worker) Close() {

}
