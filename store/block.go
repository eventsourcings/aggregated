package store

import (
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
	"strconv"
	"strings"
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
		_, nextErr := sequence.Next()
		if nextErr != nil {
			err = fmt.Errorf("new blocks failed, get origin sequence failed, %v", nextErr)
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
		blockContentSize: blockSize - 8,
		cache:            cache,
	}
	fileSize := uint64(fs.Size())
	if fileSize > uint64(blocksMetaSize) {
		endBlockNo := (fileSize-uint64(blocksMetaSize))/blockSize - 1
		bs.prepareCache(endBlockNo, uint64(maxCachedBlockNum))
	}

	return
}

type Blocks struct {
	meta             map[string]string
	file             *os.File
	sequence         Sequence
	blockSize        uint64
	blockContentSize uint64
	cache            *ristretto.Cache
}

func (bs *Blocks) prepareCache(endBlockNo uint64, maxCachedBlockNum uint64) {
	if maxCachedBlockNum > 10000 {
		maxCachedBlockNum = 10000
	}
	for i := uint64(0); i < maxCachedBlockNum; i++ {
		fileOffset := int64(endBlockNo*bs.blockSize) + blocksMetaSize
		block := make([]byte, bs.blockSize)
		n, readErr := bs.file.ReadAt(block, fileOffset)
		if readErr != nil {
			if readErr == io.EOF {
				return
			}
			return
		}
		if n > 0 {
			bs.cache.Set(endBlockNo, block, int64(bs.blockSize))
		}
		endBlockNo--
		if endBlockNo < 1 {
			return
		}
	}
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
	blockNum := uint64(math.Ceil(float64(contentLen) / float64(bs.blockContentSize)))
	if blockNum == 1 {
		idx, nextErr := bs.sequence.Next()
		if nextErr != nil {
			err = fmt.Errorf("blocks: write failed cause can not get next block no, %v", nextErr)
			return
		}
		block := bs.encodeBlock(p, 0)
		writeBlockErr := bs.writeBlock(block, idx)
		if writeBlockErr != nil {
			err = fmt.Errorf("blocks: write failed, %v", writeBlockErr)
			return
		}
		blockNo = idx
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
			err = fmt.Errorf("blocks: write failed cause can not get next block no, %v", nextErr)
			return
		}
		idxs[i] = idx
		if i > 1 {
			bs.updateBlockStickyNo(blocks[i-1], idx)
		}
		blocks[i] = bs.encodeBlock(blockContent, 0)
	}
	bs.updateBlockStickyNo(blocks[0], idxs[1])
	bs.updateBlockStickyNo(blocks[blockNum-2], idxs[blockNum-1])
	for i, block := range blocks {
		writeBlockErr := bs.writeBlock(block, idxs[i])
		if writeBlockErr != nil {
			err = fmt.Errorf("blocks: write failed, %v", writeBlockErr)
			return
		}
	}
	blockNo = idxs[0]
	return
}

func (bs *Blocks) writeBlock(block []byte, blockNo uint64) (err error) {
	fileOffset := int64(blockNo*bs.blockSize) + blocksMetaSize
	_, writeErr := bs.file.WriteAt(block, fileOffset)
	if writeErr != nil {
		err = fmt.Errorf("blocks: write %d block failed, %v", blockNo, writeErr)
		return
	}
	bs.cache.Set(blockNo, block, int64(bs.blockSize))
	return
}

func (bs *Blocks) Read(blockNo uint64) (p []byte, has bool, err error) {
	block, hasBlock, readErr := bs.readBlock(blockNo)
	if readErr != nil {
		err = readErr
		return
	}
	if !hasBlock {
		return
	}
	content, stickyNo := bs.decodeBlock(block)
	p = append(p, content...)
	if stickyNo > 0 {
		sub, hasSub, readSubErr := bs.Read(stickyNo)
		if readSubErr != nil {
			err = fmt.Errorf("blocks: read %d block sticks failed, %v", blockNo, readSubErr)
			return
		}
		if !hasSub {
			err = fmt.Errorf("blocks: read %d block sticks failed, next stick is empty", blockNo)
			return
		}
		p = append(p, sub...)
	}
	has = true
	return
}

func (bs *Blocks) readBlock(blockNo uint64) (block []byte, has bool, err error) {
	cached, hasCached := bs.cache.Get(blockNo)
	if hasCached {
		block = cached.([]byte)
		has = true
		return
	}
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
	_ = bs.file.Sync()
	_ = bs.file.Close()
	return
}

func (bs *Blocks) encodeBlock(p []byte, stickyNo uint64) (b []byte) {
	b = make([]byte, bs.blockSize)
	binary.LittleEndian.PutUint64(b[0:8], stickyNo)
	copy(b[8:], p)
	return
}

func (bs *Blocks) decodeBlock(b []byte) (p []byte, stickyNo uint64) {
	stickyNo = binary.LittleEndian.Uint64(b[0:8])
	p = b[8:]
	return
}

func (bs *Blocks) getBlockStickyNo(block []byte) (stickyNo uint64) {
	stickyNo = binary.LittleEndian.Uint64(block[0:8])
	return
}

func (bs *Blocks) updateBlockStickyNo(block []byte, stickyNo uint64) {
	binary.LittleEndian.PutUint64(block[0:8], stickyNo)
	return
}
