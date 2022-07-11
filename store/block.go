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

func (bn *BlockNo) Set(n uint64) {
	if n > bn.Value() {
		atomic.StoreUint64(&bn.value, n)
	}
	return
}

type BlockMeta map[string]string

func (meta BlockMeta) setBlockCapacity(v uint64) {
	meta["blockCapacity"] = fmt.Sprintf("%d", v)
	return
}

func (meta BlockMeta) blockCapacity() (v uint64) {
	capacity, hasCapacity := meta["blockCapacity"]
	if !hasCapacity {
		panic(fmt.Errorf("blocks: there is no blockCapacity in meta"))
		return
	}
	var err error
	v, err = strconv.ParseUint(capacity, 10, 64)
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
	BlockCapacity uint64
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
		meta:                 BlockMeta{},
		file:                 file,
		blockCapacity:        0,
		blockContentCapacity: 0,
		counter:              sync.WaitGroup{},
		cache:                cache,
		lastBlockNo:          nil,
		lastWroteBlockNo:     nil,
		closed:               0,
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
		bs.blockCapacity = bs.meta.blockCapacity()
		// blockContentSize
		bs.blockContentCapacity = bs.blockCapacity - 16
		// last block no
		bs.lastBlockNo = &BlockNo{
			value:   uint64((fs.Size() - blocksMetaSize) / int64(bs.blockCapacity)),
			padding: [7]uint64{},
		}
		bs.lastWroteBlockNo = &BlockNo{
			value:   uint64((fs.Size() - blocksMetaSize) / int64(bs.blockCapacity)),
			padding: [7]uint64{},
		}
	} else {
		// blockSize
		if options.BlockCapacity < 64*commons.BYTE {
			err = fmt.Errorf("open blocks failed, block size cant be less than 64B, current is %v", bs.blockCapacity)
			return
		}
		bs.blockCapacity = options.BlockCapacity
		bs.meta.setBlockCapacity(bs.blockCapacity)
		// blockContentSize
		bs.blockContentCapacity = bs.blockCapacity - 16
		// last block no
		bs.lastBlockNo = &BlockNo{
			value:   0,
			padding: [7]uint64{},
		}
		bs.lastWroteBlockNo = &BlockNo{
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
	maxCachedBlockNum = int64(math.Ceil(float64(maxCachedSize) / float64(bs.blockCapacity)))
	bs.cache.UpdateMaxCost(maxCachedBlockNum)
	// prepare cache
	if bs.lastWroteBlockNo.Value() > 0 {
		cachedLength := uint64(maxCachedBlockNum)
		if bs.lastWroteBlockNo.Value() < cachedLength {
			cachedLength = bs.lastWroteBlockNo.Value()
		}
		cachedOffset := bs.lastWroteBlockNo.Value() - cachedLength + 1
		_, listErr := bs.Entries(cachedOffset, cachedLength)
		if listErr != nil {
			err = fmt.Errorf("open blocks failed, prepare cached failed, %v", listErr)
			return
		}
	}
	return
}

type Blocks struct {
	meta                 BlockMeta
	file                 *os.File
	blockCapacity        uint64
	blockContentCapacity uint64
	counter              sync.WaitGroup
	cache                *ristretto.Cache
	lastBlockNo          *BlockNo
	lastWroteBlockNo     *BlockNo
	closed               int64
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
	blockNumber := uint64(math.Ceil(float64(contentLen) / float64(bs.blockContentCapacity)))
	beg, end := bs.lastBlockNo.Next(blockNumber)
	entryBeginBlockNo = beg
	entryEndBlockNo = end
	if blockNumber == 1 {
		block := NewBlock(bs.blockCapacity, 0, 0)
		writeBlockErr := bs.writeBlock(block, entryBeginBlockNo)
		if writeBlockErr != nil {
			bs.counter.Done()
			err = fmt.Errorf("blocks: write failed, %v", writeBlockErr)
			return
		}
		bs.lastWroteBlockNo.Set(beg)
		bs.cache.Set(entryBeginBlockNo, p, int64(len(p)))
		bs.counter.Done()
		return
	}
	blockNo := entryBeginBlockNo
	for i := uint64(0); i < blockNumber; i++ {
		begIdx := i * bs.blockContentCapacity
		endIdx := contentLen
		if i != blockNumber-1 {
			endIdx = begIdx + bs.blockContentCapacity
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
		block := NewBlock(bs.blockCapacity, prev, next)
		block.Write(blockContent)
		writeBlockErr := bs.writeBlock(block, blockNo)
		if writeBlockErr != nil {
			bs.counter.Done()
			err = fmt.Errorf("blocks: write failed, %v", writeBlockErr)
			return
		}
		blockNo++
	}
	bs.lastWroteBlockNo.Set(beg)
	bs.cache.Set(entryBeginBlockNo, &Entry{
		begBlockNo: beg,
		endBlockNo: end,
		value:      p,
	}, int64(len(p)))
	bs.counter.Done()
	return
}

func (bs *Blocks) writeBlock(block []byte, blockNo uint64) (err error) {
	blockLen := len(block)
	fileOffset := int64((blockNo-1)*bs.blockCapacity) + blocksMetaSize
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
		offset = entry.endBlockNo + 1
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
				lastEntry, hasLast, readLastErr := bs.Read(bs.lastWroteBlockNo.Value())
				if readLastErr != nil || !hasLast {
					break
				}
				if startBlockNo > lastEntry.begBlockNo {
					break
				}
				length := lastEntry.begBlockNo - startBlockNo + 1
				list, listErr := bs.Entries(startBlockNo, length)
				if listErr != nil {
					break
				}
				if list == nil || len(list) == 0 {
					break
				}
				entriesCh <- list
				startBlockNo = lastEntry.endBlockNo + 1
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
	if blockNo > bs.lastWroteBlockNo.Value() {
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
	prev, next, content := block.Read()
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
		_, next, content = nextBlock.Read()
		_, _ = buf.Write(content)
		endBlock++
	}
	entry = &Entry{
		begBlockNo: blockNo,
		endBlockNo: endBlock,
		value:      buf.Bytes(),
	}
	has = true
	return
}

func (bs *Blocks) readBlock(blockNo uint64) (block Block, has bool, err error) {
	fileOffset := int64((blockNo-1)*bs.blockCapacity) + blocksMetaSize
	stored := make([]byte, bs.blockCapacity)
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

func NewBlock(capacity uint64, prevNo uint64, nextNo uint64) (b Block) {
	b = make([]byte, capacity)
	binary.LittleEndian.PutUint64(b[0:8], prevNo)
	binary.LittleEndian.PutUint64(b[8:16], nextNo)
	return
}

type Block []byte

func (b Block) Write(p []byte) {
	copy(b[16:], p)
	return
}

func (b Block) Read() (prev uint64, next uint64, p []byte) {
	prev = binary.LittleEndian.Uint64(b[0:8])
	next = binary.LittleEndian.Uint64(b[8:16])
	p = b[16:]
	return
}

type Entry struct {
	begBlockNo uint64
	endBlockNo uint64
	value      []byte
}

func (e *Entry) BlockNo() (beg uint64, end uint64) {
	beg, end = e.begBlockNo, e.endBlockNo
	return
}

func (e *Entry) Value() (p []byte) {
	p = e.value
	return
}

func (e *Entry) String() (p string) {
	p = fmt.Sprintf("{[%d:%d] [%s]}", e.begBlockNo, e.endBlockNo, string(e.value))
	return
}

type EntryList []*Entry

func (p EntryList) Len() int           { return len(p) }
func (p EntryList) Less(i, j int) bool { return p[i].begBlockNo < p[j].begBlockNo }
func (p EntryList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p *EntryList) Append(e *Entry) {
	pos := sort.Search(len(*p), func(i int) bool {
		pp := *p
		return pp[i].begBlockNo >= e.begBlockNo
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
		nos = append(nos, fmt.Sprintf("{%d:%d}", entry.begBlockNo, entry.endBlockNo))
	}
	return "[" + strings.Join(nos, ",") + "]"
}
