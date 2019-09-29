package db

import (
	"errors"
	"fmt"
	"os"
	"pandadb/compaction"
	"pandadb/memtable"
)

var Panda DB

type DB struct {
	name    string
	path    string
	mem     *memtable.MemTable
	options *Options
}

func (db *DB) Open() error {
	if info, err := os.Stat(db.path); err != nil {
		if err != os.ErrNotExist {
			if err := os.Mkdir(db.path, os.ModePerm); err != nil {
				return err
			}
		}
	} else {
		if !info.IsDir() {
			notDir := fmt.Sprintf("path %s not dir!", db.path)
			return errors.New(notDir)
		}
		if info.Mode()&0x077 == 0 {
			permissionErr := fmt.Sprintf("path %s permission faild!", db.path)
			return errors.New(permissionErr)
		}
	}
	compaction.Init()
	db.mem.Open()
	return nil
}

func (db *DB) Close() {
	db.mem.Close()
}

func (db *DB) Set(key, value string) {
	db.mem.Set(key, value)
}

func (db *DB) Get(key string) (string, bool) {
	return db.mem.Get(key)
}

func NewPanda(name, path string, options *Options) *DB {
	Panda.name = name
	Panda.path = path
	Panda.options = options
	Panda.mem = memtable.NewMemTable()
	return &Panda
}
