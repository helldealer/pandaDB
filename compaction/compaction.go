package compaction

import "pandadb/table"

type CompactWorker struct{}

func (c *CompactWorker) Init() {}

//compact files from sst mature files
func (c *CompactWorker) Run() {

	table.SstTables.Merge(WorkerP.path)
}

func (c *CompactWorker) Close() {}
