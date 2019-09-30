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
	options *Options
	wp      *compaction.WorkerPool
	wal     *log.WalLogger
	pool    *memtable.Pool
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
	return nil
}

func (db *DB) Close() {
	db.wp.Close()
	db.wal.Close()
}

//叫分区可能更合适，一个section是一组相关联的表的集合，它们功用一个memTable结构，在一个区中实现事务会更加高效，因为只竞争一把锁就好。
//另外事务实现为2pl模式
func (db *DB) NewSection(name string) *memtable.Section {
	s := memtable.NewSection(name)
	if db.pool.RegSection(name, s) {
		return s
	}
	return nil
}

func (db *DB) GetSection(name string) *memtable.Section {
	return db.pool.GetSection(name)
}

func (db *DB) Set(name, key, value string) bool {
	s := db.GetSection(name)
	if s == nil {
		return false
	}
	s.Set(key, value)
	return true
}

func (db *DB) Get(name, key string) (string, bool) {
	s := db.pool.GetSection(name)
	if s == nil {
		return "", false
	}
	return s.Get(key)
}

func (db *DB) GetNameAndPath() (string, string) {
	return db.name, db.path
}

func NewPanda(name, path string, options *Options) *DB {
	Panda.name = name
	Panda.path = path
	Panda.options = options
	Panda.wp = &compaction.WorkerP
	Panda.wp.SetPath(path)
	Panda.wal = &log.Wal
	Panda.wal.SetPath(path)
	Panda.pool = memtable.NewPool()
	return &Panda
}
