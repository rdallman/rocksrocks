package rocks

import (
	"encoding/binary"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/dchest/siphash"
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
	var memtable_memory_budget uint64 = 512 * 1024 * 1024
	opts.SetCreateIfMissing(true)
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeUniversalStyleCompaction(memtable_memory_budget)

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(2048 * 1024 * 1024))
	bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(20))
	opts.SetBlockBasedTableFactory(bbto)

	opts.SetMaxOpenFiles(-1)
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(8))
	opts.SetMemtablePrefixBloomBits(100000000)
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

func (r *rocksdb) Queue(name string) queue.Queue {
	return &Queue{name: name, rocksdb: r}
}

func (r *rocksdb) Destroy() {
	r.Close()
	gorocksdb.DestroyDb(r.dir, gorocksdb.NewDefaultOptions())
}

func (q *Queue) Count() uint64 {
	return q.head - q.tail
}

func (q *Queue) key(id uint64) []byte {
	var k [16]byte
	binary.LittleEndian.PutUint64(k[:], siphash.Hash(0, 0, []byte(q.name)))
	binary.BigEndian.PutUint64(k[8:], id)
	return k[:]
}

func (q *Queue) id(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[8:])
}

var (
	wo = gorocksdb.NewDefaultWriteOptions()
	ro = gorocksdb.NewDefaultReadOptions()
)

func (q *Queue) Enqueue(body []byte) error {
	return q.rocksdb.Put(wo, q.key(atomic.AddUint64(&q.tail, 1)), body)
}

func (q *Queue) Dequeue() (*queue.Message, error) {
	wb := gorocksdb.NewWriteBatch()

	it := q.NewIterator(ro)
	defer it.Close()
	it.Seek(q.key(0))
	if !it.Valid() {
		return nil, nil
	}
	k := it.Key().Data()
	wb.Delete(k)
	if err := q.Write(wo, wb); err != nil {
		return nil, err
	}

	return &queue.Message{
		Id:   q.id(k),
		Body: it.Value().Data(),
	}, nil
}
