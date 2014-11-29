package level

import (
	"errors"
	"os"
	"runtime"

	"code.google.com/p/go-leveldb"
	"github.com/rdallman/rocksrocks/queue"
	"gopkg.in/inconshreveable/log15.v2"
)

type ldb struct {
	*leveldb.DB
	dir string
}

func NewStore() *ldb {
	runtime.GOMAXPROCS(4)
	opts := leveldb.NewOptions()
	opts.SetCreateIfMissing(true)

	opts.SetCache(leveldb.NewLRUCache(2048 * 1024 * 1024))
	opts.SetFilterPolicy(leveldb.NewBloomFilter(20))

	opts.SetMaxOpenFiles(-1)
	opts.SetWriteBufferSize(128 * 1024 * 1024)

	if err := os.MkdirAll("./tmpdata", 0755); err != nil {
		log15.Crit("couldn't mkdir", "err", err)
	}
	db, err := leveldb.Open("./tmpdata", opts)
	if err != nil {
		log15.Crit("bad things", "err", err)
		os.Exit(1)
	}
	return &ldb{DB: db, dir: "./tmpdata"}
}

func (l *ldb) Destroy() {
	l.Close()
	leveldb.DestroyDatabase(l.dir, leveldb.NewOptions())
}

func (l *ldb) Write(wb queue.WriteBatch) error {
	w, ok := wb.(*WriteBatch)
	if !ok {
		return errors.New("wrong type of wb")
	}
	return l.DB.Write(wo, w.WriteBatch)
}

func (l *ldb) Put(k, v []byte) error {
	return l.DB.Put(wo, k, v)
}

func (l *ldb) NewWriteBatch() queue.WriteBatch { return &WriteBatch{leveldb.NewWriteBatch()} }

type WriteBatch struct {
	*leveldb.WriteBatch
}

func (wb *WriteBatch) Merge(k, v []byte) {} // just don't

type Iterator struct {
	*leveldb.Iterator
}

var (
	wo = leveldb.NewWriteOptions()
	ro = leveldb.NewReadOptions()
)

func (l *ldb) NewIterator() queue.Iterator { return &Iterator{l.DB.NewIterator(ro)} }
