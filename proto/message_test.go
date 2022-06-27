package proto_test

import (
	"encoding/binary"
	"fmt"
	"github.com/eventsourcings/aggregated/proto"
	"net"
	"testing"
)

func TestTcpListen(t *testing.T) {
	server, err := net.Listen("tcp", ":8087")
	if err != nil {
		t.Fatal(err)
	}
	for {
		conn, acceptErr := server.Accept()
		if acceptErr != nil {
			fmt.Println(acceptErr)
			return
		}
		for {
			method, body, readErr := proto.ReadMessage(conn)
			if readErr != nil {
				fmt.Println(readErr)
				break
			}
			fmt.Println(method, string(body))
		}
	}
}

func TestTcpConnect(t *testing.T) {
	conn, err := net.Dial("tcp", "127.0.0.1:8087")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(proto.WriteMessage(1, []byte("0123456789"), conn))
	fmt.Println(proto.WriteMessage(1, []byte(""), conn))
	body := []byte("0123456789")
	p := make([]byte, 16+len(body))
	binary.LittleEndian.PutUint64(p[0:8], uint64(len(body)))
	binary.LittleEndian.PutUint64(p[8:16], 0)
	copy(p[16:], body)
	fmt.Println(conn.Write(p[:1]))
	fmt.Println(conn.Write(p[1:8]))
	fmt.Println(conn.Write(p[8:]))
	s := make([]byte, (16+len(body))*3)
	for i := 0; i < 3; i++ {
		p = make([]byte, 24+len(body))
		binary.LittleEndian.PutUint64(p[0:8], uint64(len(body)))
		binary.LittleEndian.PutUint64(p[8:16], uint64(i+1))
		copy(p[16:], body)
		copy(s[i*(16+len(body)):], p)
	}
	fmt.Println(conn.Write(s))
	conn.Close()
}
