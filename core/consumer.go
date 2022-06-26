package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/eventsourcings/aggregated/commons"
	"github.com/valyala/bytebufferpool"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

const (
	Ping          = uint64(1)
	Pong          = uint64(2)
	PushEvents    = uint64(3)
	EventsHandled = uint64(4)
)

type ConsumeResult struct {
	lock   sync.Mutex
	closed bool
	ch     chan bool
}

func (r *ConsumeResult) Done() (ch <-chan bool) {
	ch = r.ch
	return
}

func (r *ConsumeResult) succeed() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closed {
		return
	}
	r.ch <- true
	close(r.ch)
}

func (r *ConsumeResult) failed() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.closed {
		return
	}
	r.ch <- false
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

/*write
Message
+---------------------------------------------------------+-----------+
| Head                                                    | Body      |
+---------------------+-----------------+-----------------+-----------+
| 8(LittleEndian)     | 8(LittleEndian) | 8(LittleEndian) | n         |
+---------------------+-----------------+-----------------+-----------+
| Id                  | Method          | Len(Body)       | Body      |
+---------------------+-----------------+-----------------+-----------+
*/
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
	head := make([]byte, 24)
	binary.LittleEndian.PutUint64(head[0:8], requestId)
	binary.LittleEndian.PutUint64(head[8:16], method)
	binary.LittleEndian.PutUint64(head[16:24], uint64(len(p)))
	buf := bytebufferpool.Get()
	_, writeHead := buf.Write(head)
	if writeHead != nil {
		bytebufferpool.Put(buf)
		err = fmt.Errorf("consumer[%s]: make head failed, %v", client.id, writeHead)
		return
	}
	if len(p) > 0 {
		_, writeBody := buf.Write(p)
		if writeBody != nil {
			bytebufferpool.Put(buf)
			err = fmt.Errorf("consumer[%s]: make body failed, %v", client.id, writeBody)
			return
		}
	}
	content := buf.Bytes()
	bytebufferpool.Put(buf)
	result = &ConsumeResult{
		lock:   sync.Mutex{},
		closed: false,
		ch:     make(chan bool),
	}
	client.results.Store(requestId, result)
	n, writeErr := client.conn.Write(content)
	if writeErr != nil {
		client.results.Delete(requestId)
		close(result.ch)
		result = nil
		err = fmt.Errorf("consumer[%s]: write to conn failed, %v", client.id, writeErr)
		return
	}
	if n != len(content) {
		client.results.Delete(requestId)
		close(result.ch)
		result = nil
		err = fmt.Errorf("consumer[%s]: write to conn failed, full content is %d, but %d writed", client.id, len(content), n)
		return
	}
	return
}

func (client *ConsumerClient) Ping() (ok bool) {

	return
}

func (client *ConsumerClient) Pong() {

	return
}

func (client *ConsumerClient) Push(events *Events) (ok bool) {
	// todo
	// todo write(push, encode(events))
	// todo ok = <- result.Done()
	return
}

func (client *ConsumerClient) listen() {
	ctx, cancel := context.WithCancel(context.TODO())
	client.stop = cancel
	go func(ctx context.Context, client *ConsumerClient) {
		p := make([]byte, 4096)
		littleEndianBlock := make([]byte, 8)
		requestId := uint64(0)
		method := uint64(0)
		bodyLen := uint64(0)
		buf := bytes.NewBuffer(make([]byte, 0, 4*commons.MEGABYTE))
		for {
			stopped := false
			select {
			case <-ctx.Done():
				stopped = true
				break
			default:
				n, readErr := client.conn.Read(p)
				if readErr == io.EOF {
					client.close()
					break
				}
				buf.Write(p[0:n])
				for {
					if requestId == 0 {
						if buf.Len() < 8 {
							break
						}
						_, _ = buf.Read(littleEndianBlock)
						requestId = binary.LittleEndian.Uint64(littleEndianBlock)
					}
					if method == 0 {
						if buf.Len() < 8 {
							break
						}
						_, _ = buf.Read(littleEndianBlock)
						method = binary.LittleEndian.Uint64(littleEndianBlock)
					}
					if bodyLen == 0 {
						if buf.Len() < 8 {
							break
						}
						_, _ = buf.Read(littleEndianBlock)
						bodyLen = binary.LittleEndian.Uint64(littleEndianBlock)
						if bodyLen == 0 {
							client.handle(requestId, method, []byte{})
							requestId = 0
							method = 0
							bodyLen = 0
							break
						}
					}
					if uint64(buf.Len()) < bodyLen {
						break
					}
					body := make([]byte, bodyLen)
					_, _ = buf.Read(body)
					client.handle(requestId, method, body)
					requestId = 0
					method = 0
					bodyLen = 0
				}
				if buf.Len() == 0 {
					buf.Reset()
				}
			}
			if stopped {
				break
			}
		}
	}(ctx, client)
}

func (client *ConsumerClient) handle(requestId uint64, method uint64, body []byte) {
	if method == Ping {
		client.Pong()
		return
	}
	result0, has := client.results.LoadAndDelete(requestId)
	if !has {
		return
	}
	result, ok := result0.(*ConsumeResult)
	if !ok {
		return
	}
	switch method {
	case Pong:
		result.succeed()
		break
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

func (client *ConsumerClient) close() {
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
	Store         badger.DB
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
	id := fmt.Sprintf("%d", consumer.clients.Size())
	// todo
	client := &ConsumerClient{
		id:      id,
		lock:    sync.Mutex{},
		conn:    conn,
		closed:  false,
		closeCh: consumer.clientCloseCh,
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
