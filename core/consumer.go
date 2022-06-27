package core

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/eventsourcings/aggregated/commons"
	"net"
	"sync"
	"sync/atomic"
)

type ConsumeResult struct {
	lock   sync.Mutex
	closed bool
	ch     chan bool
}

func (r *ConsumeResult) done() (succeed bool) {
	result, ok := <-r.ch
	if !ok {
		return
	}
	succeed = result
	return
}

func (r *ConsumeResult) succeed() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closed {
		return
	}
	r.ch <- true
	r.closed = true
	close(r.ch)
}

func (r *ConsumeResult) failed() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closed {
		return
	}
	r.ch <- false
	r.closed = true
	close(r.ch)
}

type ConsumerClient struct {
	id              string
	lock            sync.Mutex
	latestRequestId uint64
	results         sync.Map
	conn            net.Conn
	closed          bool
	stop            func()
	closeCh         chan<- string
}

func (client *ConsumerClient) Key() (key string) {
	key = client.id
	return
}

func (client *ConsumerClient) write(method uint64, p []byte) (result *ConsumeResult, err error) {
	if p == nil {
		p = []byte{}
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.closed {
		err = fmt.Errorf("consumer[%s]: cant write to a closed conn", client.id)
		return
	}
	requestId := atomic.AddUint64(&client.latestRequestId, 1)
	result = &ConsumeResult{
		lock:   sync.Mutex{},
		closed: false,
		ch:     make(chan bool),
	}
	writeErr := WriteMessage(requestId, method, p, client.conn)
	if writeErr != nil {
		client.results.Delete(requestId)
		result.failed()
		result = nil
		err = fmt.Errorf("consumer[%s]: write to conn failed, %v", client.id, writeErr)
		return
	}
	// todo remove result
	// todo return reader channel, if timeout then ping, if not pong , return err can close client
	return
}

func (client *ConsumerClient) Push(events *Events) (ok bool, err error) {
	if events == nil {
		return
	}
	p, encodeErr := events.Encode()
	if encodeErr != nil {
		err = fmt.Errorf("consumer[%s]: push events failed, %v", client.id, encodeErr)
		return
	}
	result, writeErr := client.write(PushEvents, p)
	if writeErr != nil {
		err = fmt.Errorf("consumer[%s]: push events failed, %v", client.id, writeErr)
		return
	}
	ok = result.done()
	return
}

func (client *ConsumerClient) listen() {
	ctx, cancel := context.WithCancel(context.TODO())
	client.stop = cancel
	go func(ctx context.Context, client *ConsumerClient) {
		for {
			stopped := false
			select {
			case <-ctx.Done():
				stopped = true
				break
			default:
				requestId, method, body, readErr := ReadMessage(client.conn)
				if readErr != nil {
					client.Close()
					break
				}
				client.handle(requestId, method, body)
			}
			if stopped {
				break
			}
		}
	}(ctx, client)
}

func (client *ConsumerClient) handle(requestId uint64, method uint64, body []byte) {
	result0, has := client.results.LoadAndDelete(requestId)
	if !has {
		return
	}
	result, ok := result0.(*ConsumeResult)
	if !ok {
		return
	}
	switch method {
	case EventsHandled:
		if body[0] == '1' {
			result.succeed()
		} else {
			result.failed()
		}
		break
	default:
		// undefined method
		result.failed()
		break
	}
	return
}

func (client *ConsumerClient) Close() {
	client.lock.Lock()
	if client.closed {
		client.lock.Unlock()
		return
	}
	client.closed = true
	client.lock.Unlock()
	client.stop()
	_ = client.conn.Close()
	client.closeCh <- client.id
	// close all results
	client.results.Range(func(key, value interface{}) bool {
		result := value.(*ConsumeResult)
		result.failed()
		return true
	})
	client.results = sync.Map{}
}

type Consumer struct {
	Name          string
	store         badger.DB
	lock          sync.Mutex
	clients       *commons.Ring
	clientCloseCh chan string
}

func (consumer *Consumer) AppendClient(conn net.Conn) {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()
	if conn == nil {
		return
	}
	client := &ConsumerClient{
		id:              fmt.Sprintf("%d", consumer.clients.Size()),
		lock:            sync.Mutex{},
		latestRequestId: 0,
		results:         sync.Map{},
		conn:            conn,
		closed:          false,
		stop:            nil,
		closeCh:         consumer.clientCloseCh,
	}
	consumer.clients.Append(client)
	client.listen()
}

func (consumer *Consumer) listenClientClose() {
	go func(consumer *Consumer) {
		for {
			id, ok := <-consumer.clientCloseCh
			if !ok {
				break
			}
			client := consumer.clients.Get(id)
			if client != nil {
				consumer.clients.Remove(client)
			}
		}
	}(consumer)
}

func (consumer *Consumer) LatestEvent() {

}

func (consumer *Consumer) OnlineClients() {

}

func (consumer *Consumer) PushEvents(events *Events) (err error) {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()

	return
}

func (consumer *Consumer) Close() {

}
