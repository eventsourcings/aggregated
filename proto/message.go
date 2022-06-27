package proto

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	Ping          = uint64(1)
	Pong          = uint64(2)
	PushEvents    = uint64(100)
	EventsHandled = uint64(101)
)

/*WriteMessage
Message
+--------------------------- -----------+-----------+
| Head                                  | Body      |
+---------------------+-----------------+-----------+
| 8(LittleEndian)     | 8(LittleEndian) | n         |
+---------------------+----------------- -----------+
| Len(Body)           | Method          | Body      |
+---------------------+-----------------+-----------+
*/
func WriteMessage(method uint64, body []byte, writer io.Writer) (err error) {
	p := make([]byte, 16+len(body))
	binary.LittleEndian.PutUint64(p[0:8], uint64(len(body)))
	binary.LittleEndian.PutUint64(p[8:16], method)
	copy(p[16:], body)
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

func ReadMessage(reader io.Reader) (method uint64, body []byte, err error) {
	p := make([]byte, 16)
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
			bodyLen := binary.LittleEndian.Uint64(p[0:8])
			method = binary.LittleEndian.Uint64(p[8:16])
			if method == 0 {
				err = fmt.Errorf("message: read failed cause invalid method")
				return
			}
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
