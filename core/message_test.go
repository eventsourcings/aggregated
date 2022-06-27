package core_test

import (
	"encoding/binary"
	"fmt"
	"github.com/eventsourcings/aggregated/core"
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
			id, method, body, readErr := core.ReadMessage(conn)
			if readErr != nil {
				fmt.Println(readErr)
				break
			}
			fmt.Println(id, method, string(body))
		}
	}
}

func TestTcpConnect(t *testing.T) {
	conn, err := net.Dial("tcp", "127.0.0.1:8087")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(core.WriteMessage(1, 1, []byte("0123456789"), conn))
	fmt.Println(core.WriteMessage(2, 1, []byte(""), conn))
	body := []byte("0123456789")
	p := make([]byte, 24+len(body))
	binary.LittleEndian.PutUint64(p[0:8], 3)
	binary.LittleEndian.PutUint64(p[8:16], 1)
	binary.LittleEndian.PutUint64(p[16:24], uint64(len(body)))
	copy(p[24:], body)
	fmt.Println(conn.Write(p[:1]))
	fmt.Println(conn.Write(p[1:24]))
	fmt.Println(conn.Write(p[24:]))
	s := make([]byte, (24+len(body))*3)
	for i := 0; i < 3; i++ {
		p = make([]byte, 24+len(body))
		binary.LittleEndian.PutUint64(p[0:8], 3+uint64(i)+1)
		binary.LittleEndian.PutUint64(p[8:16], 1)
		binary.LittleEndian.PutUint64(p[16:24], uint64(len(body)))
		copy(p[24:], body)
		copy(s[i*(24+len(body)):], p)
	}
	fmt.Println(conn.Write(s))
	conn.Close()
}
