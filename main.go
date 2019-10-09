package main

import (
	"fmt"
	"pandadb/db"
	"time"
)

func main() {

	panda := db.NewPanda("panda", "./tmp", nil)
	if err := panda.Open(); err != nil {
		panic(err)
	}
	sc := panda.NewSection("lhr")
	start := time.Now()
	for j:=0; j<100; j++ {
		for i := 0; i < 600; i++ {
			s := fmt.Sprintf("%dkeykeykey", i)
			sc.Set(s, s)
		}
		for i := 0; i < 1000; i++ {
			_, _ = sc.Get("name")
		}
	}
	panda.Close()
	time.Sleep(1 * time.Second)
	fmt.Println(time.Since(start).String())
}