package core_test

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBytes(t *testing.T) {
	buf := bytes.NewBufferString("")
	buf.WriteString("0123456")
	p := make([]byte, 5)
	fmt.Println(buf.Read(p))
	fmt.Println(string(p))
	fmt.Println(buf.Len(), buf.Cap())
	fmt.Println(buf.Read(p))
	fmt.Println(string(p[:2]))
	fmt.Println(buf.Len(), buf.Cap())
}
