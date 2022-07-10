package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/eventsourcings/aggregated/commons"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/valyala/bytebufferpool"
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
	"time"
)

const (
	blocksMetaSize = int64(4096)
)

type BlockNo struct {
	value   uint64
	padding [7]uint64
}

func (bn *BlockNo) Value() (v uint64) {
	v = atomic.LoadUint64(&bn.value)
	return
}

func (bn *BlockNo) Next(n uint64) (beg uint64, end uint64) {
	end = atomic.AddUint64(&bn.value, n)
	beg = end - n + 1
	return
}

type BlockMeta map[string]string

func (meta BlockMeta) setBlockSize(v uint64) {
	meta["blockSize"] = fmt.Sprintf("%d", v)
	return
}

func (meta BlockMeta) blockSize() (v uint64) {
	blockSizeValue, hasBlockSize := meta["blockSize"]
	if !hasBlockSize {
		panic(fmt.Errorf("blocks: there is no blockSize in meta"))
		return
	}
	var err error
	v, err = strconv.ParseUint(blockSizeValue, 10, 64)
	if err != nil {
		panic(fmt.Errorf("blocks: blockSize in meta is not uint64"))
		return
	}
	return
}

func (meta BlockMeta) setId(v string) {
	meta["id"] = v
	return
}

func (meta BlockMeta) id() (v string) {
	id, hasId := meta["id"]
	if !hasId {
		panic(fmt.Errorf("blocks: there is no id in meta"))
		return
	}
	v = id
	return
}

type BlocksOpenOptions struct {
	Path          string
	BlockSize     uint64
	MaxCachedSize uint64
	Meta          map[string]string
}

func OpenBlocks(options BlocksOpenOptions) (bs *Blocks, err error) {
	path := strings.TrimSpace(options.Path)
	if path == "" {
		err = fmt.Errorf("open blocks failed, path is required")
		return
	}
	path, err = filepath.Abs(path)
	if err != nil {
		err = fmt.Errorf("open blocks failed, get not get absolute representation of path, %v", err)
		return
	}
	file, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if openErr != nil {
		err = fmt.Errorf("open blocks failed, open %s failed, %v", path, openErr)
		return
	}
	fs, statErr := file.Stat()
	if statErr != nil {
		err = fmt.Errorf("open blocks failed, get %s stat failed, %v", path, statErr)
		return
	}
	if fs.Mode().IsDir() {
		err = fmt.Errorf("open blocks failed, %s must be file", path)
		return
	}
	if fs.Mode().Perm()&0666 == 0 {
		err = fmt.Errorf("open blocks failed, %s perm be 0666", path)
		return
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
	maxCachedBlockNum := int64(math.Ceil(float64(maxCachedSize) / float64(1*commons.KILOBYTE)))
	cache, cacheErr := ristretto.NewCache(&ristretto.Config{
		NumCounters:        maxCachedBlockNum * 10,
		MaxCost:            int64(maxCachedSize),
		BufferItems:        64,
		Metrics:            false,
		IgnoreInternalCost: true,
	})
	if cacheErr != nil {
		err = fmt.Errorf("open blocks failed, make cache failed, %v", cacheErr)
		return
	}
	bs = &Blocks{
		meta:             BlockMeta{},
		file:             file,
		blockSize:        0,
		blockContentSize: 0,
		counter:          sync.WaitGroup{},
		cache:            cache,
		writing:          sync.Map{},
		closed:           0,
	}
	// meta
	if fs.Size() > 0 {
		metaContent := make([]byte, blocksMetaSize)
		n, readMetaErr := bs.file.ReadAt(metaContent, 0)
		if readMetaErr != nil {
			err = fmt.Errorf("open blocks failed, read meta failed, %v", readMetaErr)
			return
		}
		decodeErr := msgpack.Unmarshal(metaContent[0:n], &bs.meta)
		if decodeErr != nil {
			err = fmt.Errorf("open blocks failed, decode meta failed, %v", decodeErr)
			return
		}
		// blockSize
		bs.blockSize = bs.meta.blockSize()
		// blockContentSize
		bs.blockContentSize = bs.blockSize - 16
		// last block no
		fmt.Println(fs.Size(), fs.Size()-blocksMetaSize, uint64((fs.Size()-blocksMetaSize)/int64(bs.blockSize)))
		bs.lastBlockNo = &BlockNo{
			value:   uint64((fs.Size() - blocksMetaSize) / int64(bs.blockSize)),
			padding: [7]uint64{},
		}
	} else {
		// blockSize
		if options.BlockSize < 64*commons.BYTE {
			err = fmt.Errorf("open blocks failed, block size cant be less than 64B, current is %v", bs.blockSize)
			return
		}
		bs.blockSize = options.BlockSize
		bs.meta.setBlockSize(bs.blockSize)
		// blockContentSize
		bs.blockContentSize = bs.blockSize - 16
		// last block no
		bs.lastBlockNo = &BlockNo{
			value:   0,
			padding: [7]uint64{},
		}
		// id
		bs.meta.setId(uuid.New().String())
		// write meta
		metaContent, encodeErr := msgpack.Marshal(bs.meta)
		if encodeErr != nil {
			err = fmt.Errorf("open blocks failed, encode meta failed, %v", encodeErr)
			return
		}
		if int64(len(metaContent)) > blocksMetaSize {
			err = fmt.Errorf("open blocks failed, meta is too large")
			return
		}
		metaBlock := make([]byte, blocksMetaSize)
		copy(metaBlock, metaContent)
		_, writeMetaErr := bs.file.WriteAt(metaBlock, 0)
		if writeMetaErr != nil {
			err = fmt.Errorf("open blocks failed, write meta failed, %v", writeMetaErr)
			return
		}
		syncErr := bs.file.Sync()
		if syncErr != nil {
			err = fmt.Errorf("open blocks failed, write meta failed, %v", syncErr)
			return
		}
	}
	// update cache
	maxCachedBlockNum = int64(math.Ceil(float64(maxCachedSize) / float64(bs.blockSize)))
	bs.cache.UpdateMaxCost(maxCachedBlockNum)
	// prepare cache
	if bs.lastBlockNo.Value() > 0 {
		cachedLength := uint64(maxCachedBlockNum)
		if bs.lastBlockNo.Value() < cachedLength {
			cachedLength = bs.lastBlockNo.Value()
		}
		cachedOffset := bs.lastBlockNo.Value() - cachedLength + 1
		_, listErr := bs.Entries(cachedOffset, cachedLength)
		if listErr != nil {
			err = fmt.Errorf("open blocks failed, prepare cached failed, %v", listErr)
			return
		}
	}
	return
}

type Blocks struct {
	meta             BlockMeta
	file             *os.File
	blockSize        uint64
	blockContentSize uint64
	counter          sync.WaitGroup
	cache            *ristretto.Cache
	writing          sync.Map
	lastBlockNo      *BlockNo
	closed           int64
}

func (bs *Blocks) init() (err error) {
	fs, statErr := bs.file.Stat()
	if statErr != nil {
		err = fmt.Errorf("blocks: init failed, get %s stat failed, %v", bs.file.Name(), statErr)
		return
	}
	if fs.Mode().IsDir() {
		err = fmt.Errorf("blocks: init failed, %s must be file", bs.file.Name())
		return
	}
	if fs.Mode().Perm()&0666 == 0 {
		err = fmt.Errorf("blocks: init failed, %s perm be 0666", bs.file.Name())
		return
	}

	return
}

func (bs *Blocks) Id() (id string) {
	id = bs.meta.id()
	return
}

func (bs *Blocks) Write(p []byte) (entryBeginBlockNo uint64, entryEndBlockNo uint64, err error) {
	if atomic.LoadInt64(&bs.closed) > 0 {
		err = fmt.Errorf("blocks: write failed cause blocks was closed")
		return
	}
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
	blockNumber := uint64(math.Ceil(float64(contentLen) / float64(bs.blockContentSize)))
	beg, end := bs.lastBlockNo.Next(blockNumber)
	entryBeginBlockNo = beg
	entryEndBlockNo = end
	bs.writing.Store(entryBeginBlockNo, 0)
	if blockNumber == 1 {
		block := bs.encodeBlock(p, 0, 0)
		bs.writing.Store(entryBeginBlockNo, 1)
		writeBlockErr := bs.writeBlock(block, entryBeginBlockNo)
		if writeBlockErr != nil {
			bs.writing.Delete(entryBeginBlockNo)
			bs.counter.Done()
			err = fmt.Errorf("blocks: write failed, %v", writeBlockErr)
			return
		}
		bs.cache.Set(entryBeginBlockNo, p, int64(len(p)))
		bs.writing.Delete(entryBeginBlockNo)
		bs.counter.Done()
		return
	}
	blockNo := entryBeginBlockNo
	for i := uint64(0); i < blockNumber; i++ {
		begIdx := i * bs.blockContentSize
		endIdx := contentLen
		if i != blockNumber-1 {
			endIdx = begIdx + bs.blockContentSize
		}
		blockContent := p[begIdx:endIdx]
		prev := uint64(0)
		next := uint64(0)
		if i > 0 {
			prev = blockNo - 1
		}
		if i < blockNumber-1 {
			next = blockNo + 1
		}
		block := bs.encodeBlock(blockContent, prev, next)
		writeBlockErr := bs.writeBlock(block, blockNo)
		if writeBlockErr != nil {
			bs.writing.Delete(entryBeginBlockNo)
			bs.counter.Done()
			err = fmt.Errorf("blocks: write failed, %v", writeBlockErr)
			return
		}
		blockNo++
	}
	bs.writing.Delete(entryBeginBlockNo)
	bs.cache.Set(entryBeginBlockNo, &Entry{
		BeginBlockNo: beg,
		EndBlockNo:   end,
		Value:        p,
	}, int64(len(p)))
	bs.counter.Done()
	return
}

func (bs *Blocks) writeBlock(block []byte, blockNo uint64) (err error) {
	blockLen := len(block)
	fileOffset := int64((blockNo-1)*bs.blockSize) + blocksMetaSize
	for {
		n, writeErr := bs.file.WriteAt(block, fileOffset)
		if writeErr != nil {
			err = fmt.Errorf("blocks: write %d block failed, %v", blockNo, writeErr)
			return
		}
		if n == blockLen {
			break
		}
		block = block[n:]
		fileOffset = fileOffset + int64(n)
	}
	return
}

func (bs *Blocks) Entries(offset uint64, length uint64) (entries EntryList, err error) {
	if atomic.LoadInt64(&bs.closed) > 0 {
		err = fmt.Errorf("blocks: entries failed cause blocks was closed")
		return
	}
	if length == 0 {
		return
	}
	if offset == 0 {
		offset = 1
	}
	entries = make([]*Entry, 0, length)
	for i := uint64(0); i < length; i++ {
		entry, has, readErr := bs.Read(offset)
		if readErr != nil {
			err = fmt.Errorf("blocks: entries failed, %v", readErr)
			return
		}
		if !has {
			return
		}
		entries.Append(entry)
		offset = entry.EndBlockNo + 1
		if offset > bs.lastBlockNo.Value() {
			break
		}
	}
	return
}

func (bs *Blocks) Tail(ctx context.Context, offset uint64, interval time.Duration) (entries <-chan EntryList, stop func(), err error) {
	if atomic.LoadInt64(&bs.closed) > 0 {
		err = fmt.Errorf("blocks: tail failed cause blocks was closed")
		return
	}
	if interval < 1 {
		interval = 50 * time.Millisecond
	}
	ctx, stop = context.WithCancel(ctx)
	entriesCh := make(chan EntryList, 4096)
	go func(ctx context.Context, bs *Blocks, startBlockNo uint64, interval time.Duration, entriesCh chan EntryList) {
		for {
			stopped := false
			select {
			case <-ctx.Done():
				stopped = true
				break
			case <-time.After(interval):
				if atomic.LoadInt64(&bs.closed) > 0 {
					close(entriesCh)
					stopped = true
					break
				}
				lastEntry, hasLast, readLastErr := bs.Read(bs.lastBlockNo.Value())
				if readLastErr != nil || !hasLast {
					break
				}
				if startBlockNo > lastEntry.BeginBlockNo {
					break
				}
				length := lastEntry.BeginBlockNo - startBlockNo + 1
				list, listErr := bs.Entries(startBlockNo, length)
				if listErr != nil {
					break
				}
				if list == nil || len(list) == 0 {
					break
				}
				entriesCh <- list
				startBlockNo = lastEntry.EndBlockNo + 1
			}
			if stopped {
				break
			}
		}
	}(ctx, bs, offset, interval, entriesCh)
	entries = entriesCh
	return
}

func (bs *Blocks) Read(blockNo uint64) (entry *Entry, has bool, err error) {
	if atomic.LoadInt64(&bs.closed) > 0 {
		err = fmt.Errorf("blocks: read failed cause blocks was closed")
		return
	}
	writings, hasWriting := bs.writing.Load(blockNo)
	if hasWriting {
		for {
			if writings == 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
			writings, hasWriting = bs.writing.Load(blockNo)
		}
		entry, has, err = bs.Read(blockNo)
		return
	}
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
	block, hasBlock, readErr := bs.readBlock(blockNo)
	if readErr != nil {
		err = readErr
		return
	}
	if !hasBlock {
		return
	}
	content, prev, next := bs.decodeBlock(block)
	if prev > 0 {
		entry, has, err = bs.read(prev)
		return
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	_, _ = buf.Write(content)
	endBlock := blockNo
	for {
		if next == 0 {
			break
		}
		nextBlock, hasNextBlock, readNextErr := bs.readBlock(next)
		if readNextErr != nil {
			err = readNextErr
			return
		}
		if !hasNextBlock {
			err = fmt.Errorf("blocks: read %d of %d block failed, not exist", next, blockNo)
			return
		}
		content, _, next = bs.decodeBlock(nextBlock)
		_, _ = buf.Write(content)
		endBlock++
	}
	entry = &Entry{
		BeginBlockNo: blockNo,
		EndBlockNo:   endBlock,
		Value:        buf.Bytes(),
	}
	has = true
	return
}

func (bs *Blocks) readBlock(blockNo uint64) (block []byte, has bool, err error) {
	blockNo--
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
	atomic.StoreInt64(&bs.closed, 1)
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

type Entry struct {
	BeginBlockNo uint64
	EndBlockNo   uint64
	Value        []byte
}

type EntryList []*Entry

func (p EntryList) Len() int           { return len(p) }
func (p EntryList) Less(i, j int) bool { return p[i].BeginBlockNo < p[j].BeginBlockNo }
func (p EntryList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p *EntryList) Append(e *Entry) {
	pos := sort.Search(len(*p), func(i int) bool {
		pp := *p
		return pp[i].BeginBlockNo >= e.BeginBlockNo
	})
	if pos >= len(*p) {
		*p = append(*p, e)
	}
	return
}

func (p EntryList) Last() (entry *Entry) {
	length := len(p)
	if length == 0 {
		return
	}
	entry = p[length-1]
	return
}

func (p EntryList) String() string {
	nos := make([]string, 0, 1)
	for _, entry := range p {
		nos = append(nos, fmt.Sprintf("{%d:%d}", entry.BeginBlockNo, entry.EndBlockNo))
	}
	return "[" + strings.Join(nos, ",") + "]"
}
