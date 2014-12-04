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
	opts *gorocksdb.Options
	dir  string
}

func NewStore() *rocksdb {
	runtime.GOMAXPROCS(4)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.IncreaseParallelism(runtime.NumCPU())
	//opts.OptimizeUniversalStyleCompaction(512 * 1024 * 1024)

	//bpl=10485760;overlap=10;mcz=2;del=300000000;levels=6;ctrig=4; delay=8; stop=12; wbn=3; mbc=20;
	//mb=67108864;wbs=134217728; dds=0; sync=0; r=1000000000; t=1; vs=800; bs=65536; cs=1048576; of=500000; si=1000000;

	//--block_size=$bs --cache_size=$cs --bloom_bits=10 --cache_numshardbits=4 --open_files=$of
	//--verify_checksum=1 --sync=$sync --disable_wal=1 --compression_type=zlib --stats_interval=$si
	//--compression_ratio=50 --disable_data_sync=$dds --write_buffer_size=$wbs --target_file_size_base=$mb --max_write_buffer_number=$wbn
	//--max_background_compactions=$mbc --level0_file_num_compaction_trigger=$ctrig
	//--level0_slowdown_writes_trigger=$delay --level0_stop_writes_trigger=$stop
	//--num_levels=$levels --delete_obsolete_files_period_micros=$del --min_level_to_compress=$mcz
	//--max_grandparent_overlap_factor=$overlap --stats_per_interval=1 --max_bytes_for_level_base=$bpl --use_existing_db=0

	//opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	//opts.SetLevel0SlowdownWritesTrigger(-1) // don't
	//opts.SetLevel0StopWritesTrigger(20)     // don't

	opts.SetWriteBufferSize(64 * 1024 * 1024)
	opts.SetMaxWriteBufferNumber(3)
	opts.SetTargetFileSizeBase(64 * 1024 * 1024)
	opts.SetLevel0FileNumCompactionTrigger(8)
	opts.SetLevel0SlowdownWritesTrigger(17)
	opts.SetLevel0StopWritesTrigger(24)
	opts.SetNumLevels(4)
	opts.SetMaxBytesForLevelBase(512 * 1024 * 1024)
	opts.SetMaxBytesForLevelMultiplier(8)
	opts.SetCompression(4)

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(2048 * 1024 * 1024))
	bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(10))

	opts.SetBlockBasedTableFactory(bbto)
	opts.SetMaxOpenFiles(-1)

	//opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(8))
	opts.SetMemtablePrefixBloomBits(8 * 1024 * 1024)
	//opts.SetMergeOperator(new(counterMergeOperator))

	if err := os.MkdirAll("./tmpdata", 0755); err != nil {
		log15.Crit("couldn't mkdir", "err", err)
	}
	db, err := gorocksdb.OpenDb(opts, "./tmpdata")
	if err != nil {
		log15.Crit("bad things", "err", err)
		os.Exit(1)
	}
	return &rocksdb{DB: db, dir: "./tmpdata", opts: opts}
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
