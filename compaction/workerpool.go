package compaction

var WorkerP WorkerPool

//different tables mix in one file, make multi read fast
//make one go routine first
func Init() {
	wk := NewDumpWorker()
	wk.Init()
	go wk.Run()
}

type WorkerPool struct {
	path        string
	DumpPool    []DumpWorker
	CompactPool []CompactWorker
}

func (wp *WorkerPool) SetPath(p string) {
	wp.path = p
}

func (wp *WorkerPool) GetPath() string {
	return wp.path
}

func NewWorkerPool() *WorkerPool {
	return &WorkerPool{}
}

func (wp *WorkerPool) Close() {}
