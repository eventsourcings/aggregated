package core

import (
	"encoding/binary"
	"io"
)

const (
	Ping          = uint64(1)
	Pong          = uint64(2)
	PushEvents    = uint64(3)
	EventsHandled = uint64(4)
)

/*WriteMessage
Message
+---------------------------------------------------------+-----------+
| Head                                                    | Body      |
+---------------------+-----------------+-----------------+-----------+
| 8(LittleEndian)     | 8(LittleEndian) | 8(LittleEndian) | n         |
+---------------------+-----------------+-----------------+-----------+
| Id                  | Method          | Len(Body)       | Body      |
+---------------------+-----------------+-----------------+-----------+
*/
func WriteMessage(id uint64, method uint64, body []byte, writer io.Writer) (err error) {
	p := make([]byte, 24+len(body))
	binary.LittleEndian.PutUint64(p[0:8], id)
	binary.LittleEndian.PutUint64(p[8:16], method)
	binary.LittleEndian.PutUint64(p[16:24], uint64(len(body)))
	copy(p[24:], body)
	for {
		n, writeErr := writer.Write(p)
		if writeErr != nil {
			err = writeErr
			return
		}
		if n < len(p) {
			p = p[n:]
			continue
		}
		break
	}
	return
}

func ReadMessage(reader io.Reader) (id uint64, method uint64, body []byte, err error) {
	p := make([]byte, 24)
	headRead := false
	nn := 0
	for {
		n, readErr := reader.Read(p[nn:])
		if readErr != nil {
			err = readErr
			return
		}
		nn = nn + n
		if nn < cap(p) {
			continue
		}
		if !headRead {
			id = binary.LittleEndian.Uint64(p[0:8])
			method = binary.LittleEndian.Uint64(p[8:16])
			bodyLen := binary.LittleEndian.Uint64(p[16:24])
			headRead = true
			if bodyLen == 0 {
				return
			}
			p = make([]byte, bodyLen)
			nn = 0
			continue
		}
		body = p
		break
	}
	return
}
