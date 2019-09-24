package main

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"pandadb/db"
)

type my int

func (i my) Less(than llrb.Item) bool {
	return i < than.(my)
}

func main(){

	panda := db.NewPanda("panda", "tmp", nil)
	if err := panda.Open(); err != nil {
		panic(err)
	}
	panda.Set("name", "panda")
	name := panda.Get("name")
	fmt.Println(name)

	panda.Close()


	tree := llrb.New()
	tree.ReplaceOrInsert(my(1))
	tree.ReplaceOrInsert(my(2))
	tree.ReplaceOrInsert(my(3))
	tree.DeleteMin()
	tree.Delete(my(3))
	tree.AscendRange(my(0), my(100), func(i llrb.Item) bool {
		fmt.Println(i)
		return true
	})
}
