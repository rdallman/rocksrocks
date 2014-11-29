package rocks

import (
	"errors"
	"os"
	"runtime"
	"sync"

	"github.com/rdallman/gorocksdb"
	"github.com/rdallman/rocksrocks/queue"
	"gopkg.in/inconshreveable/log15.v2"
)

type Queue struct {
	name       string
	head, tail uint64
	sync.Mutex

	*rocksdb
}

type rocksdb struct {
	*gorocksdb.DB
	dir string
}

func NewStore() *rocksdb {
	runtime.GOMAXPROCS(4)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(2048 * 1024 * 1024))
	bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(20))
	opts.SetBlockBasedTableFactory(bbto)

	opts.SetMaxOpenFiles(-1)
	opts.SetWriteBufferSize(128 * 1024 * 1024)
	//opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(8))
	//opts.SetMemtablePrefixBloomBits(100000000)
	//opts.SetMergeOperator(new(counterMergeOperator))

	if err := os.MkdirAll("./tmpdata", 0755); err != nil {
		log15.Crit("couldn't mkdir", "err", err)
	}
	db, err := gorocksdb.OpenDb(opts, "./tmpdata")
	if err != nil {
		log15.Crit("bad things", "err", err)
		os.Exit(1)
	}
	return &rocksdb{DB: db, dir: "./tmpdata"}
}

var (
	wo = gorocksdb.NewDefaultWriteOptions()
	ro = gorocksdb.NewDefaultReadOptions()
)

func (r *rocksdb) Destroy() {
	r.Close()
	gorocksdb.DestroyDb(r.dir, gorocksdb.NewDefaultOptions())
}

func (r *rocksdb) Write(wb queue.WriteBatch) error {
	w, ok := wb.(*WriteBatch)
	if !ok {
		return errors.New("wrong type of wb")
	}
	return r.DB.Write(wo, w.WriteBatch)
}

func (r *rocksdb) Put(k, v []byte) error {
	return r.DB.Put(wo, k, v)
}

func (r *rocksdb) NewWriteBatch() queue.WriteBatch { return &WriteBatch{gorocksdb.NewWriteBatch()} }

type WriteBatch struct {
	*gorocksdb.WriteBatch
}

func (wb *WriteBatch) Close() {
	wb.Destroy()
}

type Iterator struct {
	*gorocksdb.Iterator
}

func (r *rocksdb) NewIterator() queue.Iterator { return &Iterator{r.DB.NewIterator(ro)} }

func (i *Iterator) Key() []byte {
	return makeSlice(i.Iterator.Key())
}

func (i *Iterator) Value() []byte {
	return makeSlice(i.Iterator.Value())
}

// kills the s
func makeSlice(s *gorocksdb.Slice) []byte {
	slice := make([]byte, s.Size())
	copy(slice, s.Data())
	s.Free()
	return slice
}
