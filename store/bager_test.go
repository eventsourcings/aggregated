package store_test

import (
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
	"log"
	"runtime"
	"sync"
	"testing"
)

func TestBadger(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("G:/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sequence, sequenceErr := db.GetSequence([]byte("2"), 1)
	if sequenceErr != nil {
		fmt.Println(sequenceErr)
		return
	}
	runtime.GOMAXPROCS(0)
	wg := new(sync.WaitGroup)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, sequence *badger.Sequence) {
			fmt.Println(sequence.Next())
			wg.Done()
		}(wg, sequence)
	}
	wg.Wait()
	fmt.Println(sequence.Next())
	fmt.Println(sequence.Release())
	sequence.Release()
}
