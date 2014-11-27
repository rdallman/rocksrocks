package queue

type Store interface {
	Queue(name string) Queue
	Destroy()
}

type Queue interface {
	Enqueue(body []byte) error
	Dequeue() (*Message, error)
	Count() uint64
}

type Message struct {
	Id   uint64
	Body []byte
}
