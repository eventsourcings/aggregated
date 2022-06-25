package store_test

import (
	"fmt"
	"github.com/eventsourcings/aggregated/commons"
	"github.com/eventsourcings/aggregated/store"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type FakeSequence struct {
	i int64
}

func (f *FakeSequence) Next() (idx uint64, err error) {
	idx = uint64(atomic.AddInt64(&f.i, 1))
	return
}

func (f *FakeSequence) Close() (err error) {
	return
}

func TestOpenBlocks(t *testing.T) {
	s := `G:\tmp\blocks\t1.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:          s,
		BlockSize:     64 * commons.BYTE,
		MaxCachedSize: 0,
		Sequence: &FakeSequence{
			i: -1,
		},
		Meta: map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	bs.Close()
}

func TestBlocks_Write(t *testing.T) {
	s := `G:\tmp\blocks\t1.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:          s,
		BlockSize:     64 * commons.BYTE,
		MaxCachedSize: 0,
		Sequence: &FakeSequence{
			i: -1,
		},
		Meta: map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	content := ""
	for i := 0; i < 5; i++ {
		content = content + "-" + fmt.Sprintf("%030d", i+1)
	}
	fmt.Println(bs.Write([]byte(content)[1:]))
	fmt.Println(bs.Write([]byte(time.Now().String())))
	bs.Close()
}

func TestBlocks_Write_Multi(t *testing.T) {
	wg := new(sync.WaitGroup)
	s := `G:\tmp\blocks\t2.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:          s,
		BlockSize:     64 * commons.BYTE,
		MaxCachedSize: 0,
		Sequence: &FakeSequence{
			i: -1,
		},
		Meta: map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	now := time.Now()
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, bs *store.Blocks, no int) {
			_, _ = bs.Write([]byte(fmt.Sprintf("%050d", no+1)))
			wg.Done()
		}(wg, bs, i)
	}
	wg.Wait()
	bs.Close()
	latency := time.Now().Sub(now)
	fmt.Println(latency.String(), latency/100000)
}

func TestBlocks_Read(t *testing.T) {
	s := `G:\tmp\blocks\t1.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:          s,
		BlockSize:     64 * commons.BYTE,
		MaxCachedSize: 0,
		Sequence: &FakeSequence{
			i: -1,
		},
		Meta: map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	p, has, readErr := bs.Read(1)
	fmt.Println(string(p), has, readErr)
	p, has, readErr = bs.Read(4)
	fmt.Println(string(p), has, readErr)
	fmt.Println(bs.Read(5))
}
