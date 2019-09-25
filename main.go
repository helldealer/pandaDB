package main

import (
	"fmt"
	"pandadb/db"
)

func main(){

	panda := db.NewPanda("panda", "tmp", nil)
	if err := panda.Open(); err != nil {
		panic(err)
	}
	panda.Set("name", "panda")
	name := panda.Get("name")
	fmt.Println(name)

	panda.Close()
}
