package core

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/eventsourcings/aggregated/commons"
	"github.com/eventsourcings/aggregated/proto"
	"net"
	"sync"
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
	id      string
	lock    sync.Mutex
	conn    net.Conn
	closed  bool
	stop    func()
	closeCh chan<- string
}

func (client *ConsumerClient) Key() (key string) {
	key = client.id
	return
}

func (client *ConsumerClient) Push(events *Events) (ok bool, err error) {
	if events == nil {
		return
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.closed {
		err = fmt.Errorf("consumer[%s]: cant push events to a closed conn", client.id)
		return
	}
	p, encodeErr := events.Encode()
	if encodeErr != nil {
		err = fmt.Errorf("consumer[%s]: push events failed cause encode events failed, %v", client.id, encodeErr)
		return
	}
	writeErr := proto.WriteMessage(proto.PushEvents, p, client.conn)
	if writeErr != nil {
		err = fmt.Errorf("consumer[%s]: push events to conn failed, %v", client.id, writeErr)
		return
	}
	method, body, readErr := proto.ReadMessage(client.conn)
	if readErr != nil {
		client.Close()
		err = fmt.Errorf("consumer[%s]: push events to conn succeed but receive handle result failed, %v", client.id, readErr)
		return
	}
	if method != proto.EventsHandled {
		client.Close()
		err = fmt.Errorf("consumer[%s]: push events to conn succeed but received dismatched handle result, received method is %v", client.id, method)
		return
	}
	if len(body) == 0 {
		return
	}
	ok = body[0] == '1'
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
		id:      fmt.Sprintf("%d", consumer.clients.Size()),
		lock:    sync.Mutex{},
		conn:    conn,
		closed:  false,
		stop:    nil,
		closeCh: consumer.clientCloseCh,
	}
	consumer.clients.Append(client)
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
