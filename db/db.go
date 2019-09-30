package db

import (
	"errors"
	"fmt"
	"os"
	"pandadb/compaction"
	"pandadb/log"
	"pandadb/memtable"
)

var Panda DB

type DB struct {
	name    string
	path    string
	mem     *memtable.MemTable
	options *Options
	wp  *compaction.WorkerPool
	wal *log.WalLogger
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
	log.Init()
	compaction.Init()
	db.mem.Open()
	return nil
}

func (db *DB) Close() {
	db.mem.Close()
	db.wp.Close()
	db.wal.Close()
}

func (db *DB) Set(key, value string) {
	db.wal.WriteKV(db.name, key, value)
	db.mem.Set(key, value)
}

func (db *DB) Get(key string) (string, bool) {
	return db.mem.Get(key)
}

func (db *DB) GetNameAndPath() (string, string) {
	return db.name, db.path
}

func NewPanda(name, path string, options *Options) *DB {
	Panda.name = name
	Panda.path = path
	Panda.options = options
	Panda.mem = memtable.NewMemTable(name)
	Panda.wp = &compaction.WorkerP
	Panda.wp.SetPath(path)
	Panda.wal = &log.Wal
	Panda.wal.SetPath(path)
	return &Panda
}
