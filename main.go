package main

import (
	"fmt"
	"pandadb/db"
	"reflect"
	"time"
)

func main() {

	panda := db.NewPanda("panda", "./tmp", nil)
	if err := panda.Open(); err != nil {
		panic(err)
	}
	start := time.Now()
	for j:=0; j<1; j++ {
		for i := 0; i < 600; i++ {
			s := fmt.Sprintf("%dkeykeykey", i)
			panda.Set(s, s)
		}
		for i := 0; i < 1000; i++ {
			_, _ = panda.Get("name")
		}
	}
	panda.Close()
	time.Sleep(1 * time.Second)
	fmt.Println(time.Since(start).String())
}

func Nil(o ...interface{}) {
	for _, i := range o {
		if i != nil {
			fmt.Println(reflect.TypeOf(i).String() + " is not nil")
		}
	}
}