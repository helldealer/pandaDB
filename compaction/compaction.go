package compaction

type CompactWorker struct{}

func (c *CompactWorker) Init() {}

//compact files from sst mature files
func (c *CompactWorker) Run() {

}

func (c *CompactWorker) Close() {}
