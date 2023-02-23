package timewheel

import "sync"

type queueNode[T any] struct {
	val  T
	next *queueNode[T]
}

func newNode[T any](val T) *queueNode[T] {
	return &queueNode[T]{
		val:  val,
		next: nil,
	}
}

// queue is a simply linked queue just 16 byte
// the basic unit of dequeue and enqueue is the `queueNode`
type queue[T any] struct {
	head *queueNode[T]
	tail *queueNode[T]
	lock sync.Mutex
}

func (q *queue[T]) pushNode(node *queueNode[T]) *queueNode[T] {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.head == nil {
		q.head = node
		q.tail = node
	} else {
		q.tail.next = node
		q.tail = node
	}
	return node
}
func (q *queue[T]) push(val T) (node *queueNode[T]) {
	return q.pushNode(&queueNode[T]{val: val})
}

func (q *queue[T]) pop() *queueNode[T] {
	q.lock.Lock()
	defer q.lock.Unlock()

	n := q.head
	if n == nil {
		return nil
	}
	q.head = n.next
	if q.head == nil {
		q.tail = nil
	}
	return n
}
