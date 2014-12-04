package queue_test

import (
	"bytes"
	"os/exec"
	"testing"

	"github.com/rdallman/rocksrocks/queue"
	"github.com/rdallman/rocksrocks/rocks"
	"gopkg.in/inconshreveable/log15.v2"
)

// rawr import cycles

func bod(size int) []byte {
	var buf bytes.Buffer
	for i := 0; i < size; i++ {
		buf.WriteString("a")
	}
	return buf.Bytes()
}

func benchmarkEnqueueSize(s queue.Store, n int, b *testing.B) {
	defer s.Destroy()
	body := bod(n)
	b.SetBytes(int64(len(body)))
	q := queue.New("hi")

	b.ResetTimer()
	b.SetParallelism(50)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Enqueue(s, body)
		}
	})
}

func benchmarkEnqueueN(s queue.Store, n int, b *testing.B) {
	defer s.Destroy()

	body := bod(1024 * 1024)
	q := queue.New("hi")
	for i := 0; i < n; i++ {
		q.Enqueue(s, body)
	}
	out, _ := exec.Command("du", "-h", "tmpdata").CombinedOutput()
	log15.Info(string(out))
}

//func BenchmarkEnqueueLevel1(b *testing.B) {
//benchmarkEnqueueSize(level.NewStore(), 1024*1024, b)
//}

func BenchmarkEnqueueRocks1(b *testing.B) {
	benchmarkEnqueueSize(rocks.NewStore(), 1024*1024, b)
}

//func BenchmarkEnqueueLevel1000(b *testing.B) {
//benchmarkEnqueueN(level.NewStore(), 10000, b)
//}

//func BenchmarkEnqueueRocks1000(b *testing.B) {
//benchmarkEnqueueN(rocks.NewStore(), 10000, b)
//}
