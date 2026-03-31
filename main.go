// BSEngine — A custom embedded key-value storage engine with TCP interface.
//
// Architecture overview:
//   - Slotted-page layout with linked chunk records (inspired by PostgreSQL/SQLite internals)
//   - LRU buffer pool with pin-count eviction
//   - Write-Ahead Log (WAL) with CRC32 per-entry checksums and crash replay
//   - Shadow defragmentation (copy-on-write, atomic file swap)
//   - Binary TCP protocol with semaphore-limited connection pool
//   - Graceful shutdown with full WaitGroup tracking
//
// All critical sections, race conditions, and security hardening described in
// audit_main_go_revisi.md have been addressed in this revision.

package main

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ============================================================
// 1. CONSTANTS & SYSTEM CONFIGURATION
// ============================================================

const (
	// Storage geometry
	PageSize        = 4096
	PageHeaderSize  = 32
	MaxChunkSize    = 64
	MaxSlotsPerPage = (PageSize - PageHeaderSize) / SlotSize // 1016 — hard ceiling per page
	SlotSize        = 4

	// Key / value limits — enforced at both TCP and engine layers
	MaxKeySize   = 64
	MaxValueSize = 10 * 1024 * 1024 // 10 MB

	// Buffer pool
	PoolSizeMB    = 64
	MaxCachePages = (PoolSizeMB * 1024 * 1024) / PageSize // 16 384 pages

	// File metadata
	FileVersion    uint16 = 1
	FilePermission        = 0600 // owner read/write only

	// Protocol
	Port               = ":7070"
	MaxConn            = 100
	MagicBytes  uint16 = 0xBE57
	GlobalMagic        = "BSENGINE"

	// WAL binary layout: LSN(8) + Op(1) + KeyLen(2) + ValLen(4) + Key + Val + CRC32(4)
	WALHeaderSize  = 15
	WALChecksumLen = 4

	// Janitor tuning
	JanitorInterval     = 30 * time.Second
	WALCheckpointOps    = 10_000
	DefragPageThreshold = 500

	// Network
	ConnDeadline = 60 * time.Second

	// Buffer pool capacity used during defragmentation
	defragPoolCapacity = 128
)

// Opcodes sent by the client in the TCP request header.
const (
	OpUpsert byte = 0x01
	OpView   byte = 0x02
	OpDelete byte = 0x03
	OpIncr   byte = 0x04
	OpPing   byte = 0x05
	OpStats  byte = 0x06 // [C.3] lightweight health / metrics opcode
)

// Status codes returned by the server in the TCP response header.
const (
	StatusOk       byte = 0x00
	StatusNotFound byte = 0x01
	StatusError    byte = 0x02
)

// Chunk flag byte — stored as the first byte of every encoded record.
const (
	FlagHeadSingle byte = 0x04 // single-chunk record (head + tail combined)
	FlagHead       byte = 0x01 // first chunk of a multi-chunk record
	FlagBody       byte = 0x02 // middle chunk
	FlagTail       byte = 0x03 // last chunk
	FlagTombstone  byte = 0xFF // deleted slot marker
)

// Sentinel errors — use errors.Is() for comparison, never string match.
var (
	ErrPageFull         = errors.New("engine: page full")
	ErrNotFound         = errors.New("engine: key not found")
	ErrInvalidRecord    = errors.New("engine: invalid record")
	ErrSlotNotFound     = errors.New("engine: slot not found")
	ErrPayloadTooBig    = errors.New("engine: key or value exceeds limits")
	ErrChecksumMismatch = errors.New("engine: page checksum mismatch")
	ErrPoolExhausted    = errors.New("engine: buffer pool exhausted — all pages pinned")
	ErrEvictionFailed   = errors.New("engine: dirty page eviction failed")
)

// ============================================================
// 2. CORE STRUCTS & BINARY ENCODING
// ============================================================

// Location is a (PageID, SlotID) pointer into the storage file.
type Location struct {
	PageID uint64
	SlotID uint16
}

// LinkedRecord is the in-memory representation of one chunk stored on a page.
// Large values are split into multiple LinkedRecords chained by NextLoc.
type LinkedRecord struct {
	Flags   byte
	NextLoc Location
	Key     []byte // only present on FlagHead / FlagHeadSingle
	Data    []byte
}

// PageHeader holds the parsed metadata for a single 4 KiB page.
type PageHeader struct {
	PageID      uint64
	Checksum    uint32
	PageType    uint16
	SlotCount   uint16
	LowerOffset uint16 // grows downward from PageHeaderSize
	UpperOffset uint16 // grows upward from PageSize
	FragBytes   uint16 // bytes reclaimed by tombstoning but not yet compacted
}

// Page is one 4 KiB block of storage, holding a slotted-page directory
// and variable-length record data.
type Page struct {
	Header PageHeader
	Buffer [PageSize]byte
}

// newPage initialises an empty page in memory. It does NOT touch the disk.
func newPage(pageID uint64) *Page {
	p := &Page{}
	p.Header.PageID = pageID
	p.Header.PageType = 1
	p.Header.LowerOffset = PageHeaderSize
	p.Header.UpperOffset = PageSize
	p.syncHeaderToBuffer()
	return p
}

// syncHeaderToBuffer serialises the PageHeader into the raw Buffer and
// recomputes the CRC32 checksum. Must be called just before WritePage.
func (p *Page) syncHeaderToBuffer() {
	binary.LittleEndian.PutUint64(p.Buffer[0:8], p.Header.PageID)
	binary.LittleEndian.PutUint16(p.Buffer[8:10], p.Header.PageType)
	binary.LittleEndian.PutUint16(p.Buffer[10:12], p.Header.SlotCount)
	binary.LittleEndian.PutUint16(p.Buffer[12:14], p.Header.LowerOffset)
	binary.LittleEndian.PutUint16(p.Buffer[14:16], p.Header.UpperOffset)
	binary.LittleEndian.PutUint16(p.Buffer[16:18], p.Header.FragBytes)
	p.Header.Checksum = crc32.ChecksumIEEE(p.Buffer[0:18])
	binary.LittleEndian.PutUint32(p.Buffer[18:22], p.Header.Checksum)
}

// verifyChecksum recomputes the page CRC32 and compares it against the stored
// value. Returns ErrChecksumMismatch on any discrepancy (disk corruption).
func (p *Page) verifyChecksum() error {
	stored := binary.LittleEndian.Uint32(p.Buffer[18:22])
	if computed := crc32.ChecksumIEEE(p.Buffer[0:18]); stored != computed {
		return ErrChecksumMismatch
	}
	return nil
}

// encodeRecord serialises a LinkedRecord into a compact byte slice.
// [B.11] Data length is validated against MaxChunkSize to catch programmer errors
// before a silent byte truncation could corrupt the on-disk layout.
func encodeRecord(rec *LinkedRecord) ([]byte, error) {
	if len(rec.Data) > MaxChunkSize {
		return nil, fmt.Errorf("encodeRecord: data chunk size %d exceeds MaxChunkSize %d", len(rec.Data), MaxChunkSize)
	}
	isHead := rec.Flags == FlagHead || rec.Flags == FlagHeadSingle
	size := 12 + len(rec.Data)
	if isHead {
		size += 1 + len(rec.Key)
	}
	buf := make([]byte, size)
	buf[0] = rec.Flags
	binary.LittleEndian.PutUint64(buf[1:9], rec.NextLoc.PageID)
	binary.LittleEndian.PutUint16(buf[9:11], rec.NextLoc.SlotID)

	offset := 11
	if isHead {
		buf[offset] = byte(len(rec.Key))
		offset++
		copy(buf[offset:], rec.Key)
		offset += len(rec.Key)
	}
	buf[offset] = byte(len(rec.Data))
	offset++
	copy(buf[offset:], rec.Data)
	return buf, nil
}

// decodeRecord deserialises raw bytes into a LinkedRecord.
// [B.1.3 / orig 1.3] Every length field is bounds-checked before slicing
// to prevent panic on corrupt disk data.
func decodeRecord(data []byte) (*LinkedRecord, error) {
	if len(data) < 12 {
		return nil, ErrInvalidRecord
	}
	rec := &LinkedRecord{Flags: data[0]}
	rec.NextLoc.PageID = binary.LittleEndian.Uint64(data[1:9])
	rec.NextLoc.SlotID = binary.LittleEndian.Uint16(data[9:11])

	offset := 11
	if rec.Flags == FlagHead || rec.Flags == FlagHeadSingle {
		if offset >= len(data) {
			return nil, ErrInvalidRecord
		}
		keyLen := int(data[offset])
		offset++
		if offset+keyLen > len(data) {
			return nil, ErrInvalidRecord
		}
		rec.Key = make([]byte, keyLen)
		copy(rec.Key, data[offset:offset+keyLen])
		offset += keyLen
	}
	if offset >= len(data) {
		return nil, ErrInvalidRecord
	}
	dataLen := int(data[offset])
	offset++
	if offset+dataLen > len(data) {
		return nil, ErrInvalidRecord
	}
	rec.Data = make([]byte, dataLen)
	copy(rec.Data, data[offset:offset+dataLen])
	return rec, nil
}

// insertRecord appends an encoded record into the page using the slotted-page
// layout, returning the slot ID on success or ErrPageFull.
func (p *Page) insertRecord(rec *LinkedRecord) (uint16, error) {
	encoded, err := encodeRecord(rec)
	if err != nil {
		return 0, err
	}
	recSize := uint16(len(encoded))
	if p.Header.UpperOffset-p.Header.LowerOffset < recSize+SlotSize {
		return 0, ErrPageFull
	}
	p.Header.UpperOffset -= recSize
	copy(p.Buffer[p.Header.UpperOffset:], encoded)
	slotID := p.Header.SlotCount
	binary.LittleEndian.PutUint16(p.Buffer[p.Header.LowerOffset:], p.Header.UpperOffset)
	binary.LittleEndian.PutUint16(p.Buffer[p.Header.LowerOffset+2:], recSize)
	p.Header.LowerOffset += SlotSize
	p.Header.SlotCount++
	// syncHeaderToBuffer is deferred to WritePage to avoid redundant CRC work.
	return slotID, nil
}

// getRecord reads and decodes the record at slotID.
// [B.9] Guard against uint16 overflow by enforcing MaxSlotsPerPage.
func (p *Page) getRecord(slotID uint16) (*LinkedRecord, error) {
	if slotID >= p.Header.SlotCount || slotID >= MaxSlotsPerPage {
		return nil, ErrSlotNotFound
	}
	slotPos := uint16(PageHeaderSize) + slotID*SlotSize
	recOffset := binary.LittleEndian.Uint16(p.Buffer[slotPos : slotPos+2])
	recLen := binary.LittleEndian.Uint16(p.Buffer[slotPos+2 : slotPos+4])
	if recOffset == 0 || p.Buffer[recOffset] == FlagTombstone {
		return nil, ErrSlotNotFound
	}
	return decodeRecord(p.Buffer[recOffset : recOffset+recLen])
}

// deleteRecord tombstones the slot and accounts for the freed bytes.
// [B.8] slotID is validated before any buffer access to prevent in-memory corruption.
func (p *Page) deleteRecord(slotID uint16) {
	if slotID >= p.Header.SlotCount || slotID >= MaxSlotsPerPage {
		return
	}
	slotPos := uint16(PageHeaderSize) + slotID*SlotSize
	recOffset := binary.LittleEndian.Uint16(p.Buffer[slotPos : slotPos+2])
	recLen := binary.LittleEndian.Uint16(p.Buffer[slotPos+2 : slotPos+4])
	if recOffset != 0 && p.Buffer[recOffset] != FlagTombstone {
		p.Buffer[recOffset] = FlagTombstone
		p.Header.FragBytes += recLen + SlotSize
	}
}

// ============================================================
// 3. FILE MANAGER & GLOBAL HEADER
// ============================================================

// FileManager owns the open file handle and tracks the total page count.
type FileManager struct {
	file       *os.File
	TotalPages uint64
}

// newFileManager opens (or creates) the data file and reads / initialises
// the global header stored at page 0.
func newFileManager(path string) (*FileManager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, FilePermission)
	if err != nil {
		return nil, err
	}
	fm := &FileManager{file: file}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Size() == 0 {
		if err := fm.writeGlobalHeader(); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		if err := fm.readGlobalHeader(); err != nil {
			file.Close()
			return nil, err
		}
	}
	return fm, nil
}

func (fm *FileManager) writeGlobalHeader() error {
	buf := make([]byte, PageSize)
	copy(buf[0:8], GlobalMagic)
	binary.LittleEndian.PutUint16(buf[8:10], FileVersion)
	binary.LittleEndian.PutUint16(buf[10:12], PageSize)
	binary.LittleEndian.PutUint64(buf[12:20], fm.TotalPages)
	_, err := fm.file.WriteAt(buf, 0)
	return err
}

func (fm *FileManager) readGlobalHeader() error {
	buf := make([]byte, 64)
	_, err := fm.file.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if string(buf[0:8]) == GlobalMagic {
		fm.TotalPages = binary.LittleEndian.Uint64(buf[12:20])
	}
	return nil
}

// readPage loads a page from disk, parses its header, and verifies its checksum.
func (fm *FileManager) readPage(id uint64, p *Page) error {
	if _, err := fm.file.ReadAt(p.Buffer[:], int64(id)*PageSize); err != nil {
		return err
	}
	p.Header.PageID = binary.LittleEndian.Uint64(p.Buffer[0:8])
	p.Header.SlotCount = binary.LittleEndian.Uint16(p.Buffer[10:12])
	p.Header.LowerOffset = binary.LittleEndian.Uint16(p.Buffer[12:14])
	p.Header.UpperOffset = binary.LittleEndian.Uint16(p.Buffer[14:16])
	p.Header.FragBytes = binary.LittleEndian.Uint16(p.Buffer[16:18])
	// [orig 4.6] Verify checksum on every disk read.
	if err := p.verifyChecksum(); err != nil {
		slog.Error("Page checksum mismatch", "pageID", id)
		return err
	}
	return nil
}

// writePage flushes a page to disk and updates TotalPages in the global header.
func (fm *FileManager) writePage(id uint64, p *Page) error {
	p.syncHeaderToBuffer()
	if _, err := fm.file.WriteAt(p.Buffer[:], int64(id)*PageSize); err != nil {
		return err
	}
	if id > fm.TotalPages {
		fm.TotalPages = id
		return fm.writeGlobalHeader()
	}
	return nil
}

// ============================================================
// 4. WRITE-AHEAD LOG (WAL)
// ============================================================

// WAL is an append-only log providing crash durability.
// Entry format: LSN(8) + Op(1) + KeyLen(2) + ValLen(4) + Key + Val + CRC32(4)
type WAL struct {
	mu   sync.Mutex
	lsn  uint64
	file *os.File
}

// newWAL opens (or creates) the WAL file. Returns an error — never panics on nil.
func newWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, FilePermission)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// append writes one WAL entry and calls fsync. The entry carries a CRC32
// so replayWAL can detect torn writes caused by a crash mid-write.
func (w *WAL) append(op byte, key string, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lsn++
	entryLen := WALHeaderSize + len(key) + len(val)
	buf := make([]byte, entryLen+WALChecksumLen)
	binary.LittleEndian.PutUint64(buf[0:8], w.lsn)
	buf[8] = op
	binary.LittleEndian.PutUint16(buf[9:11], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[11:15], uint32(len(val)))
	copy(buf[15:], key)
	copy(buf[15+len(key):], val)
	binary.LittleEndian.PutUint32(buf[entryLen:], crc32.ChecksumIEEE(buf[:entryLen]))
	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	return w.file.Sync() // fsync for crash safety
}

// checkpoint truncates the WAL.
// MUST only be called after the data file has been fsynced.
func (w *WAL) checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	_, err := w.file.Seek(0, 0)
	return err
}

// ============================================================
// 5. BUFFER POOL (LRU with pin-count eviction)
// ============================================================

// cachedPage is one entry in the buffer pool.
type cachedPage struct {
	Page     *Page
	Dirty    bool
	PinCount int
}

// BufferPool manages a fixed-capacity in-memory LRU cache of disk pages.
// Callers must call Unpin after each FetchPage to allow eviction.
type BufferPool struct {
	mu       sync.Mutex
	fm       *FileManager
	pages    map[uint64]*list.Element
	lru      *list.List
	capacity int
}

func newBufferPool(fm *FileManager, capacity int) *BufferPool {
	return &BufferPool{
		fm:       fm,
		pages:    make(map[uint64]*list.Element),
		lru:      list.New(),
		capacity: capacity,
	}
}

// fetchPage returns the page for id, loading it from disk on a cache miss.
// [B.4] Returns ErrEvictionFailed if a dirty page cannot be written during
// eviction (prevents silent data loss). Returns ErrPoolExhausted if every
// cached page is currently pinned and capacity is at its limit.
func (bp *BufferPool) fetchPage(id uint64) (*cachedPage, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Cache hit — promote to MRU and increment pin.
	if elem, ok := bp.pages[id]; ok {
		bp.lru.MoveToFront(elem)
		cp := elem.Value.(*cachedPage)
		cp.PinCount++
		return cp, nil
	}

	// Cache miss — evict LRU unpinned page if at capacity.
	if len(bp.pages) >= bp.capacity {
		evicted := false
		for e := bp.lru.Back(); e != nil; e = e.Prev() {
			cp := e.Value.(*cachedPage)
			if cp.PinCount > 0 {
				continue
			}
			if cp.Dirty {
				if err := bp.fm.writePage(cp.Page.Header.PageID, cp.Page); err != nil {
					slog.Error("Buffer pool: dirty page eviction failed",
						"pageID", cp.Page.Header.PageID, "error", err)
					return nil, fmt.Errorf("%w: pageID=%d: %v", ErrEvictionFailed, cp.Page.Header.PageID, err)
				}
			}
			delete(bp.pages, cp.Page.Header.PageID)
			bp.lru.Remove(e)
			evicted = true
			break
		}
		if !evicted {
			return nil, ErrPoolExhausted
		}
	}

	// Load from disk (page 0 = global header; new pages return EOF which is fine).
	p := newPage(id)
	if err := bp.fm.readPage(id, p); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	cp := &cachedPage{Page: p, PinCount: 1}
	bp.pages[id] = bp.lru.PushFront(cp)
	return cp, nil
}

// unpin decrements the pin count for id. dirty=true marks the page for
// flushing to disk on the next eviction or FlushAll.
func (bp *BufferPool) unpin(id uint64, dirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if elem, ok := bp.pages[id]; ok {
		cp := elem.Value.(*cachedPage)
		if cp.PinCount > 0 {
			cp.PinCount--
		}
		if dirty {
			cp.Dirty = true
		}
	}
}

// flushAll writes every dirty page to disk. It does NOT fsync the file;
// callers must do that explicitly when durability is required.
func (bp *BufferPool) flushAll() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	for e := bp.lru.Front(); e != nil; e = e.Next() {
		cp := e.Value.(*cachedPage)
		if cp.Dirty {
			if err := bp.fm.writePage(cp.Page.Header.PageID, cp.Page); err != nil {
				slog.Error("FlushAll: failed to write page",
					"pageID", cp.Page.Header.PageID, "error", err)
				continue
			}
			cp.Dirty = false
		}
	}
}

// ============================================================
// 6. STORAGE ENGINE CORE
// ============================================================

// Engine is the top-level storage engine. All exported CRUD methods are
// thread-safe via a single RWMutex. Internal *Locked helpers assume the
// caller already holds the appropriate lock.
type Engine struct {
	mu       sync.RWMutex
	pool     *BufferPool
	wal      *WAL
	walPath  string
	dataPath string
	// [orig 1.1] map[string]Location eliminates hash-collision silent corruption
	// that existed when FNV-64 hashes were used as map keys.
	index map[string]Location
	// [B.1] activePageID is now atomic.Uint64, consistent with totalOps.
	// This eliminates the race window between insertChunk (holding e.mu.Lock)
	// and runDefrag (goroutine with its own lock acquisition sequence).
	activePageID  atomic.Uint64
	totalOps      atomic.Uint64
	defragRunning atomic.Bool // [B.6] prevents concurrent defrag runs
}

// newEngine opens the data and WAL files, rebuilds the index from disk,
// and replays any uncommitted WAL entries for crash recovery.
func newEngine(dataPath, walPath string) (*Engine, error) {
	fm, err := newFileManager(dataPath)
	if err != nil {
		return nil, fmt.Errorf("newEngine: open data file: %w", err)
	}
	bp := newBufferPool(fm, MaxCachePages)
	wal, err := newWAL(walPath)
	if err != nil {
		fm.file.Close()
		return nil, fmt.Errorf("newEngine: open WAL: %w", err)
	}
	e := &Engine{
		pool:     bp,
		wal:      wal,
		walPath:  walPath,
		dataPath: dataPath,
		index:    make(map[string]Location),
	}
	startPage := fm.TotalPages
	if startPage == 0 {
		startPage = 1 // page 0 is reserved for the global header
	}
	e.activePageID.Store(startPage)

	// Phase 1: full disk scan to rebuild the index from committed pages.
	e.recoverIndex()

	// Phase 2: replay WAL to recover operations that were logged but not yet
	// flushed to the data file before the last crash. [B.2 / C.1]
	if err := e.replayWAL(); err != nil {
		slog.Warn("WAL replay encountered errors — some recent operations may be lost", "error", err)
	}

	return e, nil
}

// recoverIndex scans every page on disk to rebuild the in-memory key→Location map.
// [B.3] When a duplicate HEAD is found for the same key, the higher PageID
// (later write) wins and a warning is emitted so operators can detect
// unexpected inconsistencies.
func (e *Engine) recoverIndex() {
	total := e.pool.fm.TotalPages
	slog.Info("Index recovery: starting full disk scan", "totalPages", total)
	var i uint64
	for i = 1; i <= total; i++ {
		p := newPage(i)
		if err := e.pool.fm.readPage(i, p); err != nil {
			slog.Warn("Index recovery: skipping unreadable page", "pageID", i, "error", err)
			continue
		}
		for s := uint16(0); s < p.Header.SlotCount && s < MaxSlotsPerPage; s++ {
			rec, err := p.getRecord(s)
			if err != nil {
				continue
			}
			if rec.Flags != FlagHead && rec.Flags != FlagHeadSingle {
				continue
			}
			key := string(rec.Key)
			if existing, exists := e.index[key]; exists {
				slog.Warn("Index recovery: duplicate HEAD record found — keeping newer",
					"key", key, "stale_page", existing.PageID, "kept_page", i)
			}
			e.index[key] = Location{PageID: i, SlotID: s}
		}
	}
	slog.Info("Index recovery: complete", "totalKeys", len(e.index))
}

// replayWAL reads the WAL file sequentially, verifying each entry's CRC32,
// and re-applies every operation to bring the index and data up to date.
// Truncated or corrupt entries stop replay (partial-write tolerance).
// [B.2 / C.1]
func (e *Engine) replayWAL() error {
	data, err := os.ReadFile(e.walPath)
	if err != nil || len(data) == 0 {
		return nil // empty or missing WAL — nothing to replay
	}

	slog.Info("WAL replay: starting", "walBytes", len(data))
	replayed := 0
	offset := 0
	for offset < len(data) {
		// Ensure we have a full header.
		if offset+WALHeaderSize > len(data) {
			slog.Warn("WAL replay: truncated entry header — stopping")
			break
		}
		op := data[offset+8]
		keyLen := int(binary.LittleEndian.Uint16(data[offset+9:]))
		valLen := int(binary.LittleEndian.Uint32(data[offset+11:]))
		entryLen := WALHeaderSize + keyLen + valLen
		end := offset + entryLen + WALChecksumLen

		if end > len(data) {
			slog.Warn("WAL replay: truncated entry body — stopping",
				"offset", offset, "needed", end, "have", len(data))
			break
		}

		// Verify per-entry CRC32.
		stored := binary.LittleEndian.Uint32(data[end-WALChecksumLen:])
		if computed := crc32.ChecksumIEEE(data[offset : end-WALChecksumLen]); stored != computed {
			slog.Warn("WAL replay: CRC32 mismatch — stopping replay to avoid corrupt state",
				"offset", offset)
			break
		}

		// Basic sanity on key/value lengths before acting.
		if keyLen == 0 || keyLen > MaxKeySize || valLen > MaxValueSize {
			slog.Warn("WAL replay: out-of-range key/value length — skipping entry",
				"op", op, "keyLen", keyLen, "valLen", valLen)
			offset = end
			continue
		}

		key := string(data[offset+WALHeaderSize : offset+WALHeaderSize+keyLen])
		val := data[offset+WALHeaderSize+keyLen : end-WALChecksumLen]

		switch op {
		case OpUpsert, OpIncr:
			if err := e.upsertLocked(key, val); err != nil {
				slog.Warn("WAL replay: upsert failed", "key", key, "error", err)
			} else {
				replayed++
			}
		case OpDelete:
			if loc, ok := e.index[key]; ok {
				e.deleteChain(loc)
				delete(e.index, key)
				replayed++
			}
		}
		offset = end
	}
	slog.Info("WAL replay: complete", "entriesReplayed", replayed)
	return nil
}

// insertChunk finds the active page with free space and writes one chunk.
// Must be called while holding e.mu.Lock().
func (e *Engine) insertChunk(rec *LinkedRecord) (Location, error) {
	for {
		id := e.activePageID.Load()
		cp, err := e.pool.fetchPage(id)
		if err != nil {
			return Location{}, err
		}
		slotID, err := cp.Page.insertRecord(rec)
		if errors.Is(err, ErrPageFull) {
			e.pool.unpin(id, false)
			e.activePageID.Add(1) // [B.1] atomic increment
			continue
		}
		if err != nil {
			e.pool.unpin(id, false)
			return Location{}, err
		}
		e.pool.unpin(id, true)
		return Location{PageID: id, SlotID: slotID}, nil
	}
}

// deleteChain tombstones every chunk in the linked record starting at startLoc.
// Must be called while holding e.mu.Lock().
func (e *Engine) deleteChain(startLoc Location) {
	curr := startLoc
	for curr.PageID != 0 {
		cp, err := e.pool.fetchPage(curr.PageID)
		if err != nil {
			break
		}
		rec, _ := cp.Page.getRecord(curr.SlotID)
		cp.Page.deleteRecord(curr.SlotID)
		e.pool.unpin(curr.PageID, true)
		if rec == nil || rec.Flags == FlagTail || rec.Flags == FlagHeadSingle {
			break
		}
		curr = rec.NextLoc
	}
}

// viewLocked reconstructs the full value by following the chunk chain.
// Must be called while holding at least e.mu.RLock().
func (e *Engine) viewLocked(startLoc Location) ([]byte, error) {
	var result []byte
	curr := startLoc
	for curr.PageID != 0 {
		cp, err := e.pool.fetchPage(curr.PageID)
		if err != nil {
			return nil, err
		}
		rec, err := cp.Page.getRecord(curr.SlotID)
		e.pool.unpin(curr.PageID, false)
		if err != nil {
			return nil, err
		}
		result = append(result, rec.Data...)
		if rec.Flags == FlagTail || rec.Flags == FlagHeadSingle {
			break
		}
		curr = rec.NextLoc
	}
	return result, nil
}

// upsertLocked splits value into chunks, writes them in reverse order
// (tail → head), then updates the index. Must be called while holding e.mu.Lock().
// [B.10] Value length is validated here so callers that bypass the TCP layer
// (e.g. replayWAL, direct library use) are also protected.
func (e *Engine) upsertLocked(key string, value []byte) error {
	if len(value) > MaxValueSize {
		return ErrPayloadTooBig
	}
	totalChunks := len(value) / MaxChunkSize
	if len(value)%MaxChunkSize != 0 || len(value) == 0 {
		totalChunks++
	}
	var nextLoc Location
	for i := totalChunks - 1; i >= 0; i-- {
		start := i * MaxChunkSize
		end := start + MaxChunkSize
		if end > len(value) {
			end = len(value)
		}
		rec := &LinkedRecord{Data: value[start:end], NextLoc: nextLoc}
		switch {
		case totalChunks == 1:
			rec.Flags = FlagHeadSingle
			rec.Key = []byte(key)
		case i == totalChunks-1:
			rec.Flags = FlagTail
		case i == 0:
			rec.Flags = FlagHead
			rec.Key = []byte(key)
		default:
			rec.Flags = FlagBody
		}
		loc, err := e.insertChunk(rec)
		if err != nil {
			return err
		}
		nextLoc = loc
	}
	if oldLoc, exists := e.index[key]; exists {
		e.deleteChain(oldLoc)
	}
	e.index[key] = nextLoc
	return nil
}

// ── Public thread-safe CRUD operations ──────────────────────────────────────

// Upsert inserts or replaces the value for key. Validates both key and value
// lengths before taking any lock. [B.10] Value limit enforced here too.
func (e *Engine) Upsert(key string, value []byte) error {
	if len(key) == 0 || len(key) > MaxKeySize {
		return ErrPayloadTooBig
	}
	if len(value) > MaxValueSize {
		return ErrPayloadTooBig
	}
	if value == nil {
		value = []byte{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.wal.append(OpUpsert, key, value); err != nil {
		return err
	}
	e.totalOps.Add(1)
	return e.upsertLocked(key, value)
}

// View returns the stored value for key, or ErrNotFound.
func (e *Engine) View(key string) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	loc, ok := e.index[key]
	if !ok {
		return nil, ErrNotFound
	}
	return e.viewLocked(loc)
}

// Delete removes the key and tombstones its chunk chain.
func (e *Engine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.wal.append(OpDelete, key, nil); err != nil {
		return err
	}
	e.totalOps.Add(1)
	loc, ok := e.index[key]
	if !ok {
		return ErrNotFound
	}
	e.deleteChain(loc)
	delete(e.index, key)
	return nil
}

// Incr atomically reads, adds delta, and writes back a little-endian int64.
// [orig 1.2] The full read-modify-write is under a single e.mu.Lock().
func (e *Engine) Incr(key string, delta int64) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var current int64
	if loc, ok := e.index[key]; ok {
		val, err := e.viewLocked(loc)
		if err == nil && len(val) >= 8 {
			current = int64(binary.LittleEndian.Uint64(val))
		}
	}
	newVal := current + delta
	newBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(newBytes, uint64(newVal))

	if err := e.wal.append(OpIncr, key, newBytes); err != nil {
		return 0, err
	}
	e.totalOps.Add(1)
	return newVal, e.upsertLocked(key, newBytes)
}

// Stats returns a snapshot of runtime metrics for the OpStats TCP opcode. [C.3]
func (e *Engine) Stats() (keys int, ops uint64, pages uint64) {
	e.mu.RLock()
	keys = len(e.index)
	e.mu.RUnlock()
	ops = e.totalOps.Load()
	pages = e.pool.fm.TotalPages
	return
}

// ============================================================
// 7. JANITOR & SHADOW DEFRAGMENTATION
// ============================================================

// startJanitor runs the periodic maintenance goroutine.
// wg.Done is deferred so main's wg.Wait() blocks until this returns. [orig 4.7]
func (e *Engine) startJanitor(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(JanitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("Janitor: stopping.")
			return
		case <-ticker.C:
			e.pool.flushAll()
			// [orig 1.5] Safe checkpoint order:
			//   1. flushAll  — dirty pages written to disk
			//   2. file.Sync — guarantee durability (fsync)
			//   3. checkpoint — truncate WAL only after fsync confirms safety
			if e.totalOps.Load() > WALCheckpointOps {
				if err := e.pool.fm.file.Sync(); err != nil {
					slog.Error("Janitor: fsync failed", "error", err)
					continue
				}
				if err := e.wal.checkpoint(); err != nil {
					slog.Error("Janitor: WAL checkpoint failed", "error", err)
				} else {
					e.totalOps.Store(0)
					slog.Info("Janitor: WAL checkpoint done.")
				}
			}
			// [B.6] CompareAndSwap prevents launching a second defrag goroutine
			// while one is still running.
			if e.pool.fm.TotalPages > DefragPageThreshold {
				if e.defragRunning.CompareAndSwap(false, true) {
					go func() {
						defer e.defragRunning.Store(false)
						e.runDefrag()
					}()
				}
			}
		}
	}
}

// runDefrag performs shadow defragmentation:
//  1. Snapshot: copy all live key/value data to a temp file under RLock.
//  2. Swap: acquire Write lock, fsync temp file, atomically rename it over
//     the original, then re-open the new file.
//
// [B.6] guarded by defragRunning so only one instance runs at a time.
// [B.7] tmpEngine is created without a WAL field; upsertLocked is called
// directly so the WAL nil pointer is never reached.
func (e *Engine) runDefrag() {
	slog.Info("Defrag: starting shadow defragmentation.")
	tmpPath := e.dataPath + ".tmp"

	tmpFm, err := newFileManager(tmpPath)
	if err != nil {
		slog.Error("Defrag: cannot create temp file", "error", err)
		return
	}
	// tmpEngine intentionally has no WAL; it is a write-only scratch buffer.
	tmpEngine := &Engine{
		pool:  newBufferPool(tmpFm, defragPoolCapacity),
		index: make(map[string]Location),
	}
	tmpEngine.activePageID.Store(1)

	// ── Snapshot phase ──────────────────────────────────────────────────────
	e.mu.RLock()
	for key, startLoc := range e.index {
		cp, err := e.pool.fetchPage(startLoc.PageID)
		if err != nil {
			continue
		}
		rec, err := cp.Page.getRecord(startLoc.SlotID)
		e.pool.unpin(startLoc.PageID, false)
		if err != nil || len(rec.Key) == 0 {
			continue
		}
		val, err := e.viewLocked(startLoc)
		if err != nil {
			slog.Warn("Defrag: skipping key — read failed", "key", key, "error", err)
			continue
		}
		if err := tmpEngine.upsertLocked(key, val); err != nil {
			slog.Warn("Defrag: skipping key — write to temp failed", "key", key, "error", err)
		}
	}
	e.mu.RUnlock()

	// ── Swap phase ──────────────────────────────────────────────────────────
	e.mu.Lock()
	defer e.mu.Unlock()

	tmpEngine.pool.flushAll()
	if err := tmpFm.file.Sync(); err != nil {
		slog.Error("Defrag: fsync temp file failed — aborting swap", "error", err)
		tmpFm.file.Close()
		os.Remove(tmpPath)
		return
	}
	tmpFm.file.Close()
	e.pool.fm.file.Close()

	if err := os.Rename(tmpPath, e.dataPath); err != nil {
		slog.Error("Defrag: atomic rename failed", "error", err)
		// Attempt to re-open the original file so the engine remains usable.
		if newFm, err2 := newFileManager(e.dataPath); err2 == nil {
			e.pool.fm = newFm
		} else {
			slog.Error("Defrag: CRITICAL — cannot re-open original data file", "error", err2)
		}
		return
	}

	newFm, err := newFileManager(e.dataPath)
	if err != nil {
		slog.Error("Defrag: failed to re-open compacted data file", "error", err)
		return
	}
	e.pool.fm = newFm
	e.activePageID.Store(tmpEngine.activePageID.Load()) // [B.1] atomic store
	if err := e.wal.checkpoint(); err != nil {
		slog.Error("Defrag: WAL checkpoint failed", "error", err)
	}
	slog.Info("Defrag: complete — storage compacted.")
}

// ============================================================
// 8. TCP PROTOCOL LAYER
// ============================================================
//
// Request wire format:  Magic(2) | Op(1) | ReqID(4) | KeyLen(1) | ValLen(4) | Key | Val
// Response wire format: Magic(2) | ReqID(4) | Status(1) | DataLen(4) | Data

// handleConnection drives the binary protocol for one persistent TCP connection.
func handleConnection(conn net.Conn, eng *Engine) {
	defer conn.Close()
	header := make([]byte, 12)

	for {
		// Rolling deadline resets on each successful request. [orig 2.4]
		if err := conn.SetDeadline(time.Now().Add(ConnDeadline)); err != nil {
			return
		}
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		if binary.LittleEndian.Uint16(header[0:2]) != MagicBytes {
			return // protocol violation — close connection
		}

		op := header[2]
		reqID := header[3:7]
		keyLen := int(header[7])
		valLen := binary.LittleEndian.Uint32(header[8:12])

		// [B.5] Validate keyLen and valLen before allocation to prevent
		// integer overflow in the penjumlahan and DoS via oversized payloads.
		if keyLen == 0 || keyLen > MaxKeySize {
			slog.Warn("TCP: rejected invalid keyLen", "keyLen", keyLen)
			return
		}
		if int(valLen) > MaxValueSize {
			slog.Warn("TCP: rejected oversized payload", "valLen", valLen)
			return
		}
		totalPayload := keyLen + int(valLen)
		if totalPayload < keyLen { // integer overflow guard
			return
		}

		payload := make([]byte, totalPayload)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}
		key := string(payload[:keyLen])
		value := payload[keyLen:]

		var status byte = StatusOk
		var respData []byte

		switch op {
		case OpPing:
			// round-trip latency probe — status OK, no data

		case OpUpsert:
			if err := eng.Upsert(key, value); err != nil {
				slog.Warn("Upsert failed", "key", key, "error", err)
				status = StatusError
			}

		case OpView:
			data, err := eng.View(key)
			switch {
			case errors.Is(err, ErrNotFound):
				status = StatusNotFound
			case err != nil:
				status = StatusError
			default:
				respData = data
			}

		case OpDelete:
			err := eng.Delete(key)
			switch {
			case errors.Is(err, ErrNotFound):
				status = StatusNotFound
			case err != nil:
				status = StatusError
			}

		case OpIncr:
			if len(value) != 8 {
				status = StatusError
				break
			}
			delta := int64(binary.LittleEndian.Uint64(value))
			newVal, err := eng.Incr(key, delta)
			if err != nil {
				status = StatusError
			} else {
				respData = make([]byte, 8)
				binary.LittleEndian.PutUint64(respData, uint64(newVal))
			}

		case OpStats: // [C.3] lightweight health opcode
			keys, ops, pages := eng.Stats()
			// Response: keys(8) + ops(8) + pages(8) = 24 bytes
			respData = make([]byte, 24)
			binary.LittleEndian.PutUint64(respData[0:8], uint64(keys))
			binary.LittleEndian.PutUint64(respData[8:16], ops)
			binary.LittleEndian.PutUint64(respData[16:24], pages)

		default:
			status = StatusError
		}

		// Write response header then optional body.
		resp := make([]byte, 11)
		binary.LittleEndian.PutUint16(resp[0:2], MagicBytes)
		copy(resp[2:6], reqID)
		resp[6] = status
		binary.LittleEndian.PutUint32(resp[7:11], uint32(len(respData)))
		if _, err := conn.Write(resp); err != nil {
			return
		}
		if len(respData) > 0 {
			if _, err := conn.Write(respData); err != nil {
				return
			}
		}
	}
}

// startTCPServer accepts connections and dispatches them to handleConnection.
// A semaphore channel enforces the MaxConn cap.
// [B.13] A closing flag prevents wg.Add calls after wg.Wait has begun,
// which would cause a panic: "WaitGroup is reused before previous Wait returns".
func startTCPServer(ctx context.Context, wg *sync.WaitGroup, eng *Engine) {
	ln, err := net.Listen("tcp", eng.listenAddr())
	if err != nil {
		slog.Error("TCP: failed to bind", "addr", eng.listenAddr(), "error", err)
		return
	}
	slog.Info("BSEngine TCP server started", "addr", eng.listenAddr())

	var closing atomic.Bool
	go func() {
		<-ctx.Done()
		closing.Store(true)
		ln.Close()
	}()

	sem := make(chan struct{}, MaxConn)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed by ctx cancellation
		}
		if closing.Load() {
			conn.Close() // refuse new connections during shutdown
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer func() { <-sem }()
			handleConnection(c, eng)
		}(conn)
	}
}

// listenAddr returns the configured TCP address. [C.2] Falls back to the
// compile-time constant Port if the environment variable is not set.
func (e *Engine) listenAddr() string {
	if addr := os.Getenv("BSENGINE_ADDR"); addr != "" {
		return addr
	}
	return Port
}

// ============================================================
// 9. MAIN ENTRY & GRACEFUL SHUTDOWN
// ============================================================

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// [C.2] Resolve data paths from environment variables, with fallbacks.
	dataPath := resolveEnv("BSENGINE_DATA_PATH", "/data/data.bin", "data.bin")
	walPath := resolveEnv("BSENGINE_WAL_PATH", "/data/wal.bin", "wal.bin")

	slog.Info("BSEngine starting", "dataPath", dataPath, "walPath", walPath)

	eng, err := newEngine(dataPath, walPath)
	if err != nil {
		slog.Error("Fatal: engine initialisation failed", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Janitor goroutine — tracked by wg so shutdown waits for it. [orig 4.7]
	wg.Add(1)
	go eng.startJanitor(ctx, &wg)

	// TCP server goroutine — also tracked so in-flight connections drain cleanly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTCPServer(ctx, &wg, eng)
	}()

	// Block until SIGINT or SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutdown signal received — draining connections...")
	cancel()  // trigger ctx.Done in Janitor and TCP server
	wg.Wait() // wait for every goroutine to finish cleanly

	// ── Final flush sequence ──────────────────────────────────────────────
	// All goroutines have exited, so no concurrent access is possible here.
	// Order is mandatory: flush → fsync → checkpoint WAL.
	eng.pool.flushAll()

	if err := eng.pool.fm.file.Sync(); err != nil {
		slog.Error("Shutdown: data file fsync failed", "error", err)
	}
	if err := eng.wal.checkpoint(); err != nil {
		slog.Error("Shutdown: WAL checkpoint failed", "error", err)
	}
	if err := eng.pool.fm.file.Close(); err != nil {
		slog.Error("Shutdown: data file close failed", "error", err)
	}
	if err := eng.wal.file.Close(); err != nil {
		slog.Error("Shutdown: WAL file close failed", "error", err)
	}

	slog.Info("BSEngine shutdown complete. All data is safe.")
}

// resolveEnv returns the value of envKey if set; otherwise checks whether
// the parent directory of primaryPath exists and returns primaryPath, or
// falls back to fallbackPath for local development. [C.2]
func resolveEnv(envKey, primaryPath, fallbackPath string) string {
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	// Determine the parent directory by scanning for the last '/'.
	dir := "."
	for i := len(primaryPath) - 1; i >= 0; i-- {
		if primaryPath[i] == '/' {
			dir = primaryPath[:i]
			break
		}
	}
	if _, err := os.Stat(dir); err == nil && dir != "." {
		return primaryPath
	}
	return fallbackPath
}
