package rocks

import (
	"bytes"
	"testing"

	"github.com/rdallman/rocksrocks/queue"
)

func benchEnqueueSize(n int, s queue.Store, b *testing.B) {
	defer s.Destroy()
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteString("a")
	}
	body := buf.Bytes()
	b.SetBytes(int64(len(body)))
	q := s.Queue("hi")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Enqueue(body)
		}
	})
}

func BenchmarkEnqueue1k(b *testing.B) {
	s := NewStore()
	benchEnqueueSize(1024, s, b)
}

func BenchmarkEnqueue256k(b *testing.B) {
	s := NewStore()
	benchEnqueueSize(1024*256, s, b)
}

func BenchmarkEnqueue1mb(b *testing.B) {
	s := NewStore()
	benchEnqueueSize(1024*1024, s, b)
}

func BenchmarkDequeue(b *testing.B) {
	s := NewStore()
	defer s.Destroy()

	q := s.Queue("hi")
	for i := 0; i < 10000; i++ {
		q.Enqueue([]byte("HI"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Dequeue()
	}
}

func TestForever(t *testing.T) {
	s := NewStore()
	defer s.Destroy()

	q := s.Queue("hi")

	go func() {
		for {
			q.Enqueue([]byte("HI"))
		}
	}()

	for {
		q.Dequeue()
	}
}
