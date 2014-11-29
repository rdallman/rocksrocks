package queue

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/dchest/siphash"
)

type Store interface {
	NewIterator() Iterator
	NewWriteBatch() WriteBatch
	Write(WriteBatch) error
	Put(k, v []byte) error
	Destroy()
}

type Iterator interface {
	Seek([]byte)
	Key() []byte
	Value() []byte
	Valid() bool
	Close()
}

type WriteBatch interface {
	Put(k, v []byte)
	Merge(k, v []byte)
	Delete([]byte)
	Close()
}

type Message struct {
	Id   uint64
	Body []byte
}

type Queue struct {
	name       string
	head, tail uint64
	sync.Mutex
}

func New(name string) *Queue {
	return &Queue{name: name}
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

func (q *Queue) Enqueue(s Store, body []byte) error {
	return s.Put(q.key(atomic.AddUint64(&q.tail, 1)), body)
}

func (q *Queue) Dequeue(s Store) (*Message, error) {
	wb := s.NewWriteBatch()
	defer wb.Close()

	it := s.NewIterator()
	defer it.Close()
	it.Seek(q.key(0))
	if !it.Valid() {
		return nil, nil
	}
	k := it.Key()
	wb.Delete(k)
	if err := s.Write(wb); err != nil {
		return nil, err
	}

	return &Message{
		Id:   q.id(k),
		Body: it.Value(),
	}, nil
}
