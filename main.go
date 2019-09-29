package main

import (
	"fmt"
	"pandadb/db"
	"reflect"
	"time"
)

func main() {

	panda := db.NewPanda("panda", "tmp", nil)
	if err := panda.Open(); err != nil {
		panic(err)
	}
	for j:=0; j<1000; j++ {
		panda.Set("name", "panda is good and bad")
		panda.Set("name1", "panda is healthy and rich")
		panda.Set("nam2", "panda")
		panda.Set("nam3", "panda")
		panda.Set("nam4", "panda")
		panda.Set("nam5", "panda")
		panda.Set("keykey", "bearrrrrrrrrrrrrrrrrrrrrrrrrrrrr")
		for i := 0; i < 1000; i++ {
			s := fmt.Sprintf("%dkeykeykey", i)
			panda.Set(s, s)
		}
		for i := 0; i < 1000; i++ {
			_, _ = panda.Get("name")
		}
		for i := 0; i < 1000; i++ {
			s := fmt.Sprintf("%dkeyddddkeykey", i)
			panda.Set(s, s)
		}
	}
	panda.Close()
	time.Sleep(1 * time.Second)

}

func Nil(o ...interface{}) {
	for _, i := range o {
		if i != nil {
			fmt.Println(reflect.TypeOf(i).String() + " is not nil")
		}
	}
}