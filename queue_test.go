package queue_test

import (
	"bytes"
	"testing"

	"github.com/rdallman/rocksrocks/queue"
	"github.com/rdallman/rocksrocks/rocks"
)

// rawr import cycles

func benchmarkEnqueueSize(s queue.Store, n int, b *testing.B) {
	defer s.Destroy()
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteString("a")
	}
	body := buf.Bytes()
	b.SetBytes(int64(len(body)))
	q := queue.New("hi")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Enqueue(s, body)
		}
	})
}

//func BenchmarkEnqueueLevel1(b *testing.B) {
//benchmarkEnqueueSize(level.NewStore(), 1024*1024, b)
//}

func BenchmarkEnqueueRocks1(b *testing.B) {
	benchmarkEnqueueSize(rocks.NewStore(), 1024*1024, b)
}
