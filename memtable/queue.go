package memtable

import (
	"container/heap"
	"sync"
)

//todo: make it configurable
const (
	InitPriorityQueueCap = 100
)

var ImRegistry *ImMemTableRegistry

func init() {
	ImRegistry = NewImTableRegistry()
}

//todo: len should make a limit, also the limit of tables
type ImMemTableRegistry struct {
	lock               sync.RWMutex
	queue              PriorityQueue
	len                int
	lastCompactionTime int64
	wait               chan struct{}
}

func (im *ImMemTableRegistry) Push(elem *Element) {
	im.lock.Lock()
	defer im.lock.Unlock()
	heap.Push(&im.queue, elem)
	if im.len == 0 {
		//Assert im.wait != nil
		close(im.wait)
	}
	im.len++
	//todo: size limit
}

func (im *ImMemTableRegistry) Pop() *Element {
	im.lock.Lock()
	defer im.lock.Unlock()
	if im.len == 0 {
		return nil
	}
	im.len--
	if im.len == 0 {
		im.wait = make(chan struct{})
	}
	return heap.Pop(&im.queue).(*Element)
}

func (im *ImMemTableRegistry) UpdatePriority(elem *Element, newPriority int) {
	im.lock.Lock()
	defer im.lock.Unlock()
	im.queue.updatePriority(elem, newPriority)
}

func (im *ImMemTableRegistry) Len() int {
	im.lock.RLock()
	defer im.lock.RUnlock()
	return im.len
}

func (im *ImMemTableRegistry) Wait() {
	im.lock.RLock()
	defer im.lock.RUnlock()
	<-im.wait
}

//not need lock, inaccuracy tolerating
func (im *ImMemTableRegistry) UpdateTime(now int64) {
	//im.lock.Lock()
	//defer im.lock.Unlock()
	im.lastCompactionTime = now
}

func (im *ImMemTableRegistry) GetUpdateTime() int64 {
	return im.lastCompactionTime
}

func NewImTableRegistry() *ImMemTableRegistry {
	return &ImMemTableRegistry{queue: make(PriorityQueue, 0, InitPriorityQueueCap),}
}

type Element struct {
	table    *ImmutableMemTable
	priority int
	index    int
}

type PriorityQueue []*Element

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(v interface{}) {
	n := len(*pq)
	elem := v.(*Element)
	elem.index = n
	*pq = append(*pq, elem)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	elem := old[n-1]
	*pq = old[0 : n-1]
	elem.index = -1
	return elem
}

//param elem must be pointer to elem in queue
func (pq *PriorityQueue) updatePriority(elem *Element, priority int) {
	elem.priority = priority
	heap.Fix(pq, elem.index)
}