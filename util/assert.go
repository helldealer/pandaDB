package util

import "reflect"

var Assert Assertion

type Assertion struct{}

func (as *Assertion) Nil(o ...interface{}) {
	for _, i := range o {
		if i != nil {
			panic(reflect.TypeOf(i).String() + " is not nil")
		}
	}
}

func (as *Assertion) NotNil(o ...interface{}) {
	for _, i := range o {
		if i == nil {
			panic(reflect.TypeOf(i).String() + " is nil")
		}
	}
}

func (as *Assertion) Equal(a,b int) {
	if a != b {
		panic("a is not equal with b")
	}
}
