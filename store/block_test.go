package store_test

import (
	"context"
	"fmt"
	"github.com/eventsourcings/aggregated/commons"
	"github.com/eventsourcings/aggregated/store"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenBlocks(t *testing.T) {
	s := `G:\tmp\blocks\t1.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:                s,
		BlockCapacity:       64 * commons.BYTE,
		MaxCachedMemorySize: 0,
		Meta:                map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	bs.Close()
}

func TestBlocks_Write(t *testing.T) {
	s := `G:\tmp\blocks\t1.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:                s,
		BlockCapacity:       64 * commons.BYTE,
		MaxCachedMemorySize: 0,
		Meta:                map[string]string{},
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
		Path:                s,
		BlockCapacity:       1 * commons.KILOBYTE,
		MaxCachedMemorySize: 0,
		Meta:                map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	now := time.Now()
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, bs *store.Blocks, no int) {
			_, _, _ = bs.Write([]byte(fmt.Sprintf("%050d", no+1)))
			wg.Done()
		}(wg, bs, i)
	}
	wg.Wait()
	bs.Close()
	latency := time.Now().Sub(now)
	fmt.Println(latency.String(), uint64(latency/100000))
}

func TestBlocks_Read(t *testing.T) {
	s := `G:\tmp\blocks\t1.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:                s,
		BlockCapacity:       64 * commons.BYTE,
		MaxCachedMemorySize: 0,
		Meta:                map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	e, has, readErr := bs.Read(1)
	fmt.Println(e, has, readErr)
	e, has, readErr = bs.Read(5)
	fmt.Println(e, has, readErr)
	e, has, readErr = bs.Read(7)
	fmt.Println(has, readErr)
}

func TestBlocks_List(t *testing.T) {
	s := `G:\tmp\blocks\t1.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:                s,
		BlockCapacity:       64 * commons.BYTE,
		MaxCachedMemorySize: 0,
		Meta:                map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	list, listErr := bs.Entries(0, 10)
	if listErr != nil {
		t.Fatal(listErr)
	}
	fmt.Println(list)
}

func TestBlocks_Tail(t *testing.T) {
	s := `G:\tmp\blocks\t1.bs`
	bs, bsErr := store.OpenBlocks(store.BlocksOpenOptions{
		Path:                s,
		BlockCapacity:       64 * commons.BYTE,
		MaxCachedMemorySize: 0,
		Meta:                map[string]string{},
	})
	if bsErr != nil {
		t.Error(bsErr)
	}
	defer bs.Close()
	wg := new(sync.WaitGroup)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(bs *store.Blocks, wg *sync.WaitGroup, i int) {
			ch, _, _, tailErr := bs.Tail(context.TODO(), 0, 1*time.Second)
			wg.Done()
			if tailErr != nil {
				fmt.Println("tail[", i, "]:", tailErr)
				return
			}
			for {
				list, ok := <-ch
				if !ok {
					fmt.Println("tail[", i, "]:", "stop")
					break
				}
				fmt.Println("tail[", i, "]:", list)
			}
		}(bs, wg, i)
	}
	wg.Wait()
	for i := 0; i < 10; i++ {
		_, bno, wErr := bs.Write([]byte(fmt.Sprintf("%d:%s", i, time.Now().String())))
		fmt.Println(bno, wErr)
	}
	time.Sleep(5 * time.Second)
	bs.Close()
	time.Sleep(5 * time.Second)
}

type BlockNo struct {
	Value   uint64
	padding [7]uint64
}

func (bn *BlockNo) Next(n uint64) (beg uint64, end uint64) {
	end = atomic.AddUint64(&bn.Value, n)
	beg = end - n + 1
	return
}

func TestBlockNo(t *testing.T) {
	wg := new(sync.WaitGroup)
	bn := &BlockNo{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(bn *BlockNo, wg *sync.WaitGroup, n uint64) {
			beg, end := bn.Next(n)
			fmt.Println(n, beg, end, end-beg)
			wg.Done()
		}(bn, wg, uint64(i+1))
	}
	wg.Wait()
	fmt.Println(bn.Value, bn.padding)
}
