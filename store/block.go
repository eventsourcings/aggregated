package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/eventsourcings/aggregated/commons"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	blocksMetaSize = int64(4096)
)

type Sequence interface {
	Next() (idx uint64, err error)
	Close() (err error)
}

type BlocksOpenOptions struct {
	Path          string
	BlockSize     uint64
	MaxCachedSize uint64
	Sequence      Sequence
	Meta          map[string]string
}

func OpenBlocks(options BlocksOpenOptions) (bs *Blocks, err error) {
	path := strings.TrimSpace(options.Path)
	if path == "" {
		err = fmt.Errorf("new blocks failed, path is required")
		return
	}
	path, err = filepath.Abs(path)
	if err != nil {
		err = fmt.Errorf("new blocks failed, get not get absolute representation of path, %v", err)
		return
	}
	file, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if openErr != nil {
		err = fmt.Errorf("new blocks failed, open %s failed, %v", path, openErr)
		return
	}
	fs, statErr := file.Stat()
	if statErr != nil {
		err = fmt.Errorf("new blocks failed, get %s stat failed, %v", path, statErr)
		return
	}
	if fs.Mode().IsDir() {
		err = fmt.Errorf("new blocks failed, %s must be file", path)
		return
	}
	if fs.Mode().Perm()&0666 == 0 {
		err = fmt.Errorf("new blocks failed, %s perm be 0666", path)
		return
	}
	sequence := options.Sequence
	if sequence == nil {
		err = fmt.Errorf("new blocks failed, sequence is required")
		return
	}
	blockSize := options.BlockSize
	meta := options.Meta
	if fs.Size() > 0 {
		metaContent := make([]byte, blocksMetaSize)
		n, readMetaErr := file.ReadAt(metaContent, 0)
		if readMetaErr != nil {
			err = fmt.Errorf("new blocks failed, read meta failed, %v", readMetaErr)
			return
		}
		metaInFile := make(map[string]string)
		decodeErr := msgpack.Unmarshal(metaContent[0:n], &metaInFile)
		if decodeErr != nil {
			err = fmt.Errorf("new blocks failed, decode meta failed, %v", decodeErr)
			return
		}
		blockSizeValue, hasBlockSize := metaInFile["blockSize"]
		if !hasBlockSize {
			err = fmt.Errorf("new blocks failed, meta is invalid")
			return
		}
		blockSize, err = strconv.ParseUint(blockSizeValue, 10, 64)
		if err != nil {
			err = fmt.Errorf("new blocks failed, get block size from meta failed, %v", err)
			return
		}
		delete(metaInFile, "blockSize")
		meta = metaInFile
	} else {
		if blockSize < 64*commons.BYTE {
			err = fmt.Errorf("new blocks failed, block size cant be less than 64B, current is %v", blockSize)
			return
		}
		meta["blockSize"] = fmt.Sprintf("%d", blockSize)
		metaContent, encodeErr := msgpack.Marshal(meta)
		if encodeErr != nil {
			err = fmt.Errorf("new blocks failed, encode meta failed, %v", encodeErr)
			return
		}
		if int64(len(metaContent)) > blocksMetaSize {
			err = fmt.Errorf("new blocks failed, meta is too large")
			return
		}
		metaBlock := make([]byte, blocksMetaSize)
		copy(metaBlock, metaContent)
		_, writeMetaErr := file.WriteAt(metaBlock, 0)
		if writeMetaErr != nil {
			err = fmt.Errorf("new blocks failed, write meta failed, %v", writeMetaErr)
			return
		}
		_ = file.Sync()
		delete(meta, "blockSize")
	}
	maxCachedSize := options.MaxCachedSize
	if maxCachedSize < 1 {
		memory, getMemoryErr := mem.VirtualMemory()
		if getMemoryErr != nil {
			maxCachedSize = 64 * commons.MEGABYTE
		} else {
			maxCachedSize = memory.Free / 8
		}
	}
	maxCachedBlockNum := int64(math.Ceil(float64(maxCachedSize) / float64(blockSize)))
	cache, cacheErr := ristretto.NewCache(&ristretto.Config{
		NumCounters:        maxCachedBlockNum * 10,
		MaxCost:            int64(maxCachedSize),
		BufferItems:        64,
		Metrics:            false,
		IgnoreInternalCost: true,
	})
	if cacheErr != nil {
		err = fmt.Errorf("new blocks failed, make cache failed, %v", cacheErr)
		return
	}

	bs = &Blocks{
		meta:             meta,
		file:             file,
		sequence:         sequence,
		blockSize:        blockSize,
		blockContentSize: blockSize - 16,
		counter:          sync.WaitGroup{},
		cache:            cache,
	}
	fileSize := uint64(fs.Size())
	if fileSize > uint64(blocksMetaSize) {
		startBlockNo := uint64(0)
		endBlockNo := (fileSize-uint64(blocksMetaSize))/blockSize - 1
		if endBlockNo+1 >= uint64(maxCachedBlockNum) {
			startBlockNo = endBlockNo + 1 - uint64(maxCachedBlockNum)
		}
		err = bs.prepareCache(startBlockNo, endBlockNo)
		if err != nil {
			err = fmt.Errorf("new blocks failed, %v", err)
			return
		}
	}
	return
}

type Blocks struct {
	meta             map[string]string
	file             *os.File
	sequence         Sequence
	blockSize        uint64
	blockContentSize uint64
	counter          sync.WaitGroup
	cache            *ristretto.Cache
	lastBlockNo      uint64
}

func (bs *Blocks) prepareCache(startBlockNo uint64, endBlockNo uint64) (err error) {
	end, hasEnd, readEndErr := bs.read(endBlockNo)
	if readEndErr != nil {
		err = fmt.Errorf("prepare caches failed, %v", readEndErr)
		return
	}
	if !hasEnd {
		return
	}
	bs.lastBlockNo = end.BlockNos[0]
	if startBlockNo > bs.lastBlockNo {
		startBlockNo = bs.lastBlockNo
	}
	entries, listErr := bs.List(startBlockNo, bs.lastBlockNo)
	if listErr != nil {
		err = fmt.Errorf("prepare caches failed, %v", listErr)
		return
	}
	for _, entry := range entries {
		bs.cache.Set(entry.BlockNos[0], entry, int64(len(entry.Value)))
	}
	return
}

func (bs *Blocks) Write(p []byte) (blockNo uint64, err error) {
	if p == nil {
		err = fmt.Errorf("blocks: write failed cause can not write nil content")
		return
	}
	contentLen := uint64(len(p))
	if contentLen == 0 {
		err = fmt.Errorf("blocks: write failed cause can not write blank content")
		return
	}
	bs.counter.Add(1)
	blockNum := uint64(math.Ceil(float64(contentLen) / float64(bs.blockContentSize)))
	if blockNum == 1 {
		idx, nextErr := bs.sequence.Next()
		if nextErr != nil {
			bs.counter.Done()
			err = fmt.Errorf("blocks: write failed cause can not get next block no, %v", nextErr)
			return
		}
		block := bs.encodeBlock(p, 0, 0)
		writeBlockErr := bs.writeBlock(block, idx)
		if writeBlockErr != nil {
			bs.counter.Done()
			err = fmt.Errorf("blocks: write failed, %v", writeBlockErr)
			return
		}
		blockNo = idx
		bs.cache.Set(blockNo, p, int64(len(p)))
		bs.counter.Done()
		if atomic.LoadUint64(&bs.lastBlockNo) < blockNo {
			atomic.StoreUint64(&bs.lastBlockNo, blockNo)
		}
		return
	}
	idxs := make([]uint64, blockNum)
	blocks := make([][]byte, blockNum)
	for i := uint64(0); i < blockNum; i++ {
		begIdx := i * bs.blockContentSize
		endIdx := contentLen
		if i != blockNum-1 {
			endIdx = begIdx + bs.blockContentSize
		}
		blockContent := p[begIdx:endIdx]
		idx, nextErr := bs.sequence.Next()
		if nextErr != nil {
			bs.counter.Done()
			err = fmt.Errorf("blocks: write failed cause can not get next block no, %v", nextErr)
			return
		}
		idxs[i] = idx
		blocks[i] = bs.encodeBlock(blockContent, 0, 0)
	}
	for i, block := range blocks {
		prev := uint64(0)
		next := uint64(0)
		if i > 0 {
			prev = idxs[i-1]
		}
		if uint64(i) < blockNum-1 {
			next = idxs[i+1]
		}
		bs.updateBlockStickyNo(block, prev, next)
		writeBlockErr := bs.writeBlock(block, idxs[i])
		if writeBlockErr != nil {
			bs.counter.Done()
			err = fmt.Errorf("blocks: write failed, %v", writeBlockErr)
			return
		}
	}
	blockNo = idxs[0]
	bs.cache.Set(blockNo, &Entry{
		BlockNos: idxs,
		Value:    p,
	}, int64(len(p)))
	if atomic.LoadUint64(&bs.lastBlockNo) < blockNo {
		atomic.StoreUint64(&bs.lastBlockNo, blockNo)
	}
	bs.counter.Done()
	return
}

func (bs *Blocks) writeBlock(block []byte, blockNo uint64) (err error) {
	bs.counter.Add(1)
	fileOffset := int64(blockNo*bs.blockSize) + blocksMetaSize
	_, writeErr := bs.file.WriteAt(block, fileOffset)
	if writeErr != nil {
		bs.counter.Done()
		err = fmt.Errorf("blocks: write %d block failed, %v", blockNo, writeErr)
		return
	}
	bs.counter.Done()
	return
}

func (bs *Blocks) List(startBlockNo uint64, endBlockNo uint64) (entries EntryList, err error) {
	if startBlockNo > atomic.LoadUint64(&bs.lastBlockNo) {
		return
	}
	entries = make([]*Entry, 0, endBlockNo-startBlockNo+1)
	currentBlockNo := startBlockNo
	for {
		if currentBlockNo > endBlockNo {
			break
		}
		entry, has, readErr := bs.Read(currentBlockNo)
		if readErr != nil {
			err = fmt.Errorf("blocks: list from %d to %d failed, %v", startBlockNo, endBlockNo, readErr)
			return
		}
		if !has {
			break
		}
		entries.Append(entry)
		vacant := entry.BlockNos.Vacant()
		if vacant == nil || len(vacant) == 0 {
			currentBlockNo = entry.BlockNos[len(entry.BlockNos)-1] + 1
			continue
		}
		vacantSegments := vacant.SuccessiveSegments()
		for _, segment := range vacantSegments {
			segmentStart := segment[0]
			segmentEnd := segment[len(segment)-1]
			vacantEntries, vacantErr := bs.List(segmentStart, segmentEnd)
			if vacantErr != nil {
				err = fmt.Errorf("blocks: list from %d to %d failed, %v", segmentStart, segmentEnd, vacantErr)
				return
			}
			if vacantEntries != nil && len(vacantEntries) > 0 {
				for _, vacantEntry := range vacantEntries {
					entries.Append(vacantEntry)
				}
			}
		}
		currentBlockNo = entry.BlockNos[len(entry.BlockNos)-1] + 1
	}
	return
}

func (bs *Blocks) Read(blockNo uint64) (entry *Entry, has bool, err error) {
	cached, hasCached := bs.cache.Get(blockNo)
	if hasCached {
		entry = cached.(*Entry)
		has = true
		return
	}
	entry, has, err = bs.read(blockNo)
	return
}

func (bs *Blocks) read(blockNo uint64) (entry *Entry, has bool, err error) {
	group := make(map[uint64][]byte)
	has, err = bs.load(blockNo, group)
	if err != nil {
		return
	}
	if !has {
		return
	}
	if len(group) == 1 {
		entry = &Entry{
			BlockNos: []uint64{blockNo},
			Value:    group[blockNo],
		}
		return
	}
	blockNos := BlockNoList(make([]uint64, 0, len(group)))
	for no := range group {
		blockNos = append(blockNos, no)
	}
	sort.Sort(blockNos)
	buf := bytes.NewBuffer([]byte{})
	for _, no := range blockNos {
		buf.Write(group[no])
	}
	entry = &Entry{
		BlockNos: blockNos,
		Value:    buf.Bytes(),
	}
	return
}

func (bs *Blocks) load(blockNo uint64, group map[uint64][]byte) (has bool, err error) {
	block, hasBlock, readErr := bs.readBlock(blockNo)
	if readErr != nil {
		err = readErr
		return
	}
	if !hasBlock {
		return
	}
	content, prev, next := bs.decodeBlock(block)
	group[blockNo] = content
	if prev > 0 {
		_, hasPrev := group[prev]
		if !hasPrev {
			hasPrevContent, prevErr := bs.load(prev, group)
			if prevErr != nil {
				err = fmt.Errorf("blocks: read prev of %d block failed, %v", blockNo, prevErr)
				return
			}
			if !hasPrevContent {
				err = fmt.Errorf("blocks: read prev of %d block failed, prev was not found", blockNo)
				return
			}
		}
	}
	if next > 0 {
		_, hasNext := group[next]
		if !hasNext {
			hasNextContent, nextErr := bs.load(next, group)
			if nextErr != nil {
				err = fmt.Errorf("blocks: read next of %d block failed, %v", blockNo, nextErr)
				return
			}
			if !hasNextContent {
				err = fmt.Errorf("blocks: read next of %d block failed, next was not found", blockNo)
				return
			}
		}
	}
	has = true
	return
}

func (bs *Blocks) readBlock(blockNo uint64) (block []byte, has bool, err error) {
	fileOffset := int64(blockNo*bs.blockSize) + blocksMetaSize
	stored := make([]byte, bs.blockSize)
	n, readErr := bs.file.ReadAt(stored, fileOffset)
	if readErr != nil {
		if readErr == io.EOF {
			return
		}
		err = fmt.Errorf("blocks: read %d block failed, %v", blockNo, readErr)
		return
	}
	has = n > 0
	if has {
		block = stored
	}
	return
}

func (bs *Blocks) UpdateCacheSize(size uint64) {
	if size < 16*commons.MEGABYTE {
		size = 16 * commons.MEGABYTE
	}
	bs.cache.UpdateMaxCost(int64(size))
	return
}

func (bs *Blocks) Close() {
	bs.counter.Wait()
	_ = bs.file.Sync()
	_ = bs.file.Close()
	return
}

func (bs *Blocks) encodeBlock(p []byte, prev uint64, next uint64) (b []byte) {
	b = make([]byte, bs.blockSize)
	binary.LittleEndian.PutUint64(b[0:8], prev)
	binary.LittleEndian.PutUint64(b[8:16], next)
	copy(b[16:], p)
	return
}

func (bs *Blocks) decodeBlock(b []byte) (p []byte, prev uint64, next uint64) {
	prev = binary.LittleEndian.Uint64(b[0:8])
	next = binary.LittleEndian.Uint64(b[8:16])
	p = b[16:]
	return
}

func (bs *Blocks) getBlockStickyNo(block []byte) (prev uint64, next uint64) {
	prev = binary.LittleEndian.Uint64(block[0:8])
	next = binary.LittleEndian.Uint64(block[8:16])
	return
}

func (bs *Blocks) updateBlockStickyNo(block []byte, prev uint64, next uint64) {
	binary.LittleEndian.PutUint64(block[0:8], prev)
	binary.LittleEndian.PutUint64(block[8:16], next)
	return
}

type BlockNoList []uint64

func (p BlockNoList) Len() int           { return len(p) }
func (p BlockNoList) Less(i, j int) bool { return p[i] < p[j] }
func (p BlockNoList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p BlockNoList) SuccessiveSegments() (segments []BlockNoList) {
	if p == nil || len(p) == 0 {
		return
	}
	if len(p) == 1 {
		segments = append(segments, p)
		return
	}
	current := BlockNoList(make([]uint64, 0, 1))
	target := p[0]
	current = append(current, target)
	for i := 1; i < len(p); i++ {
		if p[i]-target == 1 {
			current = append(current, p[i])
			target = p[i]
			continue
		}
		segments = append(segments, current)
		current = make([]uint64, 0, 1)
		target = p[i]
		current = append(current, target)
	}
	segments = append(segments, current)
	return
}

func (p BlockNoList) Vacant() (values BlockNoList) {
	if p == nil || len(p) <= 1 {
		return
	}
	target := p[0]
	for i := 1; i < len(p); i++ {
		if p[i]-target == 1 {
			target = p[i]
			continue
		}
		value := target
		for {
			value = value + 1
			if value == p[i] {
				break
			}
			values = append(values, value)
		}
		target = p[i]
	}
	return
}

type Entry struct {
	BlockNos BlockNoList
	Value    []byte
}

type EntryList []*Entry

func (p EntryList) Len() int           { return len(p) }
func (p EntryList) Less(i, j int) bool { return p[i].BlockNos[0] < p[j].BlockNos[0] }
func (p EntryList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p *EntryList) Append(e *Entry) {
	pos := sort.Search(len(*p), func(i int) bool {
		pp := *p
		return pp[i].BlockNos[0] >= e.BlockNos[0]
	})
	if pos >= len(*p) {
		*p = append(*p, e)
	}
	return
}
