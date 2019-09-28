package version

import (
	"fmt"
	"sync"
)

func init(){
	VerInfo.Init()
}

var VerInfo Info

//改成atomic ？
type Info struct {
	lock    sync.RWMutex
	version uint64 //same as fileNum, version++ when a new file create
}

func (i *Info) Init() {
	//recover from log
	fmt.Println("version init")
}

func (i *Info) GetVersion() uint64 {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.version
}

func (i *Info) SetVersion(v uint64) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.version = v
}

func (i *Info) IncByOne() uint64 {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.version++
	return i.version
}
