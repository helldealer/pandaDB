package main

import (
	"pandadb/db"
	"time"
)

func main(){
	panda := db.NewPanda("panda", "tmp", nil)
	if err := panda.Open(); err != nil {
		panic(err)
	}
	panda.Set("name", "panda is good and bad")
	panda.Set("name1", "panda is healthy and rich")
	panda.Set("nam2", "panda")
	panda.Set("nam3", "panda")
	panda.Set("nam4", "panda")
	panda.Set("nam5", "panda")
	_ = panda.Get("name")
	panda.Close()
	time.Sleep(10*time.Second)
}
