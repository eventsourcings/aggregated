package commons

import (
	"bytes"
	"sync"
)

type Keyable interface {
	Key() (key string)
}

type entry struct {
	next, prev *entry
	value      Keyable
}

func New(values ...Keyable) (r *Ring) {
	r = &Ring{
		mutex: sync.RWMutex{},
		head:  nil,
		size:  0,
	}
	if values != nil && len(values) > 0 {
		for _, value := range values {
			r.Append(value)
		}
	}
	return
}

type Ring struct {
	mutex sync.RWMutex
	head  *entry
	size  int
}

func (r *Ring) Append(v Keyable) {
	if v == nil {
		return
	}
	r.mutex.Lock()
	e := &entry{
		value: v,
	}
	if r.head == nil {
		e.next = e
		e.prev = e
		r.head = e
	} else {
		prev := r.head.prev
		prev.next = e
		e.prev = prev
		e.next = r.head
		r.head.prev = e
	}
	r.size++
	r.mutex.Unlock()
}

func (r *Ring) Remove(v Keyable) {
	if v == nil {
		return
	}
	r.mutex.Lock()
	if r.head == nil {
		r.mutex.Unlock()
		return
	}
	for i := 0; i < r.size; i++ {
		e := r.next()
		if e.value.Key() == v.Key() {
			if e.prev.value.Key() == v.Key() && e.next.value.Key() == v.Key() {
				r.head = nil
				break
			}
			prev := e.prev
			next := e.next
			prev.next = next
			break
		}
	}
	r.size--
	r.mutex.Unlock()
}

func (r *Ring) Next() (value Keyable) {
	r.mutex.RLock()
	if r.size == 0 {
		r.mutex.RUnlock()
		return
	}
	value = r.next().value
	r.mutex.RUnlock()
	return
}

func (r *Ring) Get(key string) (value Keyable) {
	r.mutex.RLock()
	if r.size == 0 {
		r.mutex.RUnlock()
		return
	}
	for i := 0; i < r.size; i++ {
		n := r.next().value
		if n.Key() == key {
			value = n
			break
		}
	}
	r.mutex.RUnlock()
	return
}

func (r *Ring) Size() (size int) {
	r.mutex.RLock()
	size = r.size
	r.mutex.RUnlock()
	return
}

func (r *Ring) String() (value string) {
	r.mutex.RLock()
	p := bytes.NewBufferString("")
	_ = p.WriteByte('[')
	for i := 0; i < r.size; i++ {
		e := r.next()
		if i == 0 {
			_, _ = p.WriteString(e.value.Key())
		} else {
			_, _ = p.WriteString(", ")
			_, _ = p.WriteString(e.value.Key())
		}
	}
	_ = p.WriteByte(']')
	value = p.String()
	r.mutex.RUnlock()
	return
}

func (r *Ring) next() (entry *entry) {
	entry = r.head
	r.head = r.head.next
	return
}
