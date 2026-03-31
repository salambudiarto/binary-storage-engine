package main

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
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

// ==========================================
// 1. CONSTANTS & SYSTEM CONFIGURATION
// ==========================================
const (
	PageSize       = 4096
	PageHeaderSize = 32
	MaxChunkSize   = 64
	MaxKeySize     = 64
	MaxValueSize   = 10 * 1024 * 1024 // 10MB max payload (DoS protection)
	SlotSize       = 4
	PoolSizeMB     = 64
	MaxCachePages  = (PoolSizeMB * 1024 * 1024) / PageSize // 16384 pages

	Port    = ":7070"
	MaxConn = 100

	FileVersion    uint16 = 1
	FilePermission        = 0600 // Owner-only read/write (was 0666)

	MagicBytes  uint16 = 0xBE57
	GlobalMagic        = "BSENGINE"

	// Janitor intervals & thresholds
	JanitorInterval     = 30 * time.Second
	WALCheckpointOps    = 10000
	DefragPageThreshold = 500

	// Connection deadline
	ConnReadDeadline = 60 * time.Second

	// WAL record structure sizes
	// LSN(8) + Op(1) + KeyLen(2) + ValLen(4) + Key + Val + Checksum(4)
	WALHeaderSize = 15
)

// Opcodes & Status
const (
	OpUpsert byte = 0x01
	OpView   byte = 0x02
	OpDelete byte = 0x03
	OpIncr   byte = 0x04
	OpPing   byte = 0x05

	StatusOk       byte = 0x00
	StatusNotFound byte = 0x01
	StatusError    byte = 0x02
)

// Chunk Flags
const (
	FlagHeadSingle byte = 0x04
	FlagHead       byte = 0x01
	FlagBody       byte = 0x02
	FlagTail       byte = 0x03
	FlagTombstone  byte = 0xFF
)

var (
	ErrPageFull         = errors.New("engine: page full")
	ErrNotFound         = errors.New("engine: key not found")
	ErrInvalidRecord    = errors.New("engine: invalid record")
	ErrSlotNotFound     = errors.New("engine: slot not found")
	ErrPayloadTooBig    = errors.New("engine: key or value exceeds limits")
	ErrChecksumMismatch = errors.New("engine: page checksum mismatch")
)

// ==========================================
// 2. CORE STRUCTS & BINARY ENCODING
// ==========================================

type Location struct {
	PageID uint64
	SlotID uint16
}

type LinkedRecord struct {
	Flags   byte
	NextLoc Location
	Key     []byte
	Data    []byte
}

type PageHeader struct {
	PageID      uint64
	Checksum    uint32
	PageType    uint16
	SlotCount   uint16
	LowerOffset uint16
	UpperOffset uint16
	FragBytes   uint16
}

type Page struct {
	Header PageHeader
	Buffer [PageSize]byte
}

// NewPage initializes a new in-memory page with the given ID.
func NewPage(pageID uint64) *Page {
	p := &Page{}
	p.Header.PageID = pageID
	p.Header.PageType = 1
	p.Header.LowerOffset = PageHeaderSize
	p.Header.UpperOffset = PageSize
	p.syncHeaderToBuffer()
	return p
}

// syncHeaderToBuffer serializes the PageHeader struct into the raw Buffer and
// recalculates the CRC32 checksum. Only call this when preparing to write to disk.
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

// verifyChecksum recomputes the CRC32 and compares against the stored value.
// Returns ErrChecksumMismatch if they differ (indicates disk corruption).
func (p *Page) verifyChecksum() error {
	stored := binary.LittleEndian.Uint32(p.Buffer[18:22])
	computed := crc32.ChecksumIEEE(p.Buffer[0:18])
	if stored != computed {
		return ErrChecksumMismatch
	}
	return nil
}

// encodeRecord serializes a LinkedRecord into a byte slice.
func encodeRecord(rec *LinkedRecord) []byte {
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
	return buf
}

// decodeRecord deserializes a byte slice into a LinkedRecord.
// Returns ErrInvalidRecord if the data is malformed or truncated.
func decodeRecord(data []byte) (*LinkedRecord, error) {
	if len(data) < 12 {
		return nil, ErrInvalidRecord
	}
	rec := &LinkedRecord{Flags: data[0]}
	rec.NextLoc.PageID = binary.LittleEndian.Uint64(data[1:9])
	rec.NextLoc.SlotID = binary.LittleEndian.Uint16(data[9:11])

	offset := 11
	isHead := rec.Flags == FlagHead || rec.Flags == FlagHeadSingle
	if isHead {
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

// InsertRecord writes an encoded record into the page using slotted-page layout.
func (p *Page) InsertRecord(rec *LinkedRecord) (uint16, error) {
	encoded := encodeRecord(rec)
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
	// Note: syncHeaderToBuffer is called only on WritePage, not on every in-memory mod.
	return slotID, nil
}

// GetRecord reads and decodes the record at the given slotID.
func (p *Page) GetRecord(slotID uint16) (*LinkedRecord, error) {
	if slotID >= p.Header.SlotCount {
		return nil, ErrSlotNotFound
	}
	slotPos := uint16(PageHeaderSize) + (slotID * SlotSize)
	recOffset := binary.LittleEndian.Uint16(p.Buffer[slotPos : slotPos+2])
	recLen := binary.LittleEndian.Uint16(p.Buffer[slotPos+2 : slotPos+4])
	if recOffset == 0 || p.Buffer[recOffset] == FlagTombstone {
		return nil, ErrSlotNotFound
	}
	return decodeRecord(p.Buffer[recOffset : recOffset+recLen])
}

// DeleteRecord marks a slot as tombstoned and tracks fragmented bytes.
func (p *Page) DeleteRecord(slotID uint16) {
	slotPos := uint16(PageHeaderSize) + (slotID * SlotSize)
	recOffset := binary.LittleEndian.Uint16(p.Buffer[slotPos : slotPos+2])
	recLen := binary.LittleEndian.Uint16(p.Buffer[slotPos+2 : slotPos+4])
	if recOffset != 0 && p.Buffer[recOffset] != FlagTombstone {
		p.Buffer[recOffset] = FlagTombstone
		p.Header.FragBytes += recLen + SlotSize
	}
}

// ==========================================
// 3. FILE MANAGER & GLOBAL HEADER
// ==========================================

type FileManager struct {
	file       *os.File
	TotalPages uint64
}

// NewFileManager opens (or creates) the data file and reads/initializes the global header.
func NewFileManager(path string) (*FileManager, error) {
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
		fm.TotalPages = 0
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

// ReadPage loads a page from disk into the provided Page struct and parses its header.
func (fm *FileManager) ReadPage(id uint64, p *Page) error {
	_, err := fm.file.ReadAt(p.Buffer[:], int64(id)*PageSize)
	if err != nil {
		return err
	}
	p.Header.PageID = binary.LittleEndian.Uint64(p.Buffer[0:8])
	p.Header.SlotCount = binary.LittleEndian.Uint16(p.Buffer[10:12])
	p.Header.LowerOffset = binary.LittleEndian.Uint16(p.Buffer[12:14])
	p.Header.UpperOffset = binary.LittleEndian.Uint16(p.Buffer[14:16])
	p.Header.FragBytes = binary.LittleEndian.Uint16(p.Buffer[16:18])

	// Verify integrity on every read from disk
	if err := p.verifyChecksum(); err != nil {
		slog.Error("Checksum mismatch detected", "pageID", id, "error", err)
		return err
	}
	return nil
}

// WritePage flushes a page to disk, syncing the header and updating TotalPages if needed.
func (fm *FileManager) WritePage(id uint64, p *Page) error {
	p.syncHeaderToBuffer()
	_, err := fm.file.WriteAt(p.Buffer[:], int64(id)*PageSize)
	if err != nil {
		return err
	}
	if id > fm.TotalPages {
		fm.TotalPages = id
		if err := fm.writeGlobalHeader(); err != nil {
			return err
		}
	}
	return nil
}

// ==========================================
// 4. WRITE-AHEAD LOG (WAL)
// ==========================================

type WAL struct {
	mu   sync.Mutex
	lsn  uint64
	file *os.File
}

// NewWAL opens (or creates) the WAL file. Returns an error if the file cannot be opened.
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, FilePermission)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// Append writes a WAL entry for crash recovery. Calls fsync after each write.
func (w *WAL) Append(op byte, key string, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lsn++
	// LSN(8) + Op(1) + KeyLen(2) + ValLen(4) + Key + Val + Checksum(4)
	buf := make([]byte, WALHeaderSize+len(key)+len(val)+4)
	binary.LittleEndian.PutUint64(buf[0:8], w.lsn)
	buf[8] = op
	binary.LittleEndian.PutUint16(buf[9:11], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[11:15], uint32(len(val)))
	copy(buf[15:], key)
	copy(buf[15+len(key):], val)
	checksum := crc32.ChecksumIEEE(buf[:WALHeaderSize+len(key)+len(val)])
	binary.LittleEndian.PutUint32(buf[WALHeaderSize+len(key)+len(val):], checksum)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	return w.file.Sync() // fsync for crash safety
}

// Checkpoint truncates the WAL after all data has been safely flushed to disk.
// MUST only be called after fm.file.Sync() to guarantee data durability.
func (w *WAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	_, err := w.file.Seek(0, 0)
	return err
}

// ==========================================
// 5. BUFFER POOL (LRU)
// ==========================================

type CachedPage struct {
	Page     *Page
	Dirty    bool
	PinCount int
}

type BufferPool struct {
	mu       sync.Mutex
	fm       *FileManager
	pages    map[uint64]*list.Element
	lru      *list.List
	capacity int
}

func NewBufferPool(fm *FileManager, capacity int) *BufferPool {
	return &BufferPool{
		fm:       fm,
		pages:    make(map[uint64]*list.Element),
		lru:      list.New(),
		capacity: capacity,
	}
}

// FetchPage retrieves a page from the cache (LRU hit) or loads it from disk (miss).
// Caller MUST call Unpin when done.
func (bp *BufferPool) FetchPage(id uint64) (*CachedPage, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if elem, ok := bp.pages[id]; ok {
		bp.lru.MoveToFront(elem)
		cPage := elem.Value.(*CachedPage)
		cPage.PinCount++
		return cPage, nil
	}

	// Evict LRU unpinned page if at capacity
	if len(bp.pages) >= bp.capacity {
		for e := bp.lru.Back(); e != nil; e = e.Prev() {
			cPage := e.Value.(*CachedPage)
			if cPage.PinCount == 0 {
				if cPage.Dirty {
					if err := bp.fm.WritePage(cPage.Page.Header.PageID, cPage.Page); err != nil {
						slog.Error("Failed to evict dirty page", "pageID", cPage.Page.Header.PageID, "error", err)
					}
				}
				delete(bp.pages, cPage.Page.Header.PageID)
				bp.lru.Remove(e)
				break
			}
		}
	}

	p := NewPage(id)
	err := bp.fm.ReadPage(id, p)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	cPage := &CachedPage{Page: p, PinCount: 1}
	bp.pages[id] = bp.lru.PushFront(cPage)
	return cPage, nil
}

// Unpin decrements the pin count of a page. Set dirty=true if the page was modified.
func (bp *BufferPool) Unpin(id uint64, dirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if elem, ok := bp.pages[id]; ok {
		cPage := elem.Value.(*CachedPage)
		if cPage.PinCount > 0 {
			cPage.PinCount--
		}
		if dirty {
			cPage.Dirty = true
		}
	}
}

// FlushAll writes all dirty pages to disk. Does not fsync the underlying file.
func (bp *BufferPool) FlushAll() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	for e := bp.lru.Front(); e != nil; e = e.Next() {
		cPage := e.Value.(*CachedPage)
		if cPage.Dirty {
			if err := bp.fm.WritePage(cPage.Page.Header.PageID, cPage.Page); err != nil {
				slog.Error("FlushAll: failed to write page", "pageID", cPage.Page.Header.PageID, "error", err)
				continue
			}
			cPage.Dirty = false
		}
	}
}

// ==========================================
// 6. STORAGE ENGINE CORE
// ==========================================

type Engine struct {
	mu   sync.RWMutex
	pool *BufferPool
	wal  *WAL
	// FIX: Use map[string]Location to eliminate hash collision (silent data corruption).
	// Previously map[uint64]Location with FNV-64a could silently overwrite entries.
	index        map[string]Location
	activePageID uint64
	totalOps     atomic.Uint64
	dataPath     string
}

func NewEngine(dataPath, walPath string) (*Engine, error) {
	fm, err := NewFileManager(dataPath)
	if err != nil {
		return nil, err
	}
	bp := NewBufferPool(fm, MaxCachePages)
	wal, err := NewWAL(walPath)
	if err != nil {
		fm.file.Close()
		return nil, err
	}

	e := &Engine{
		pool:         bp,
		wal:          wal,
		index:        make(map[string]Location),
		activePageID: fm.TotalPages,
		dataPath:     dataPath,
	}
	if e.activePageID == 0 {
		e.activePageID = 1 // Page 0 is reserved for the global header
	}
	e.recoverIndex()
	return e, nil
}

// recoverIndex performs a full disk scan on startup to rebuild the in-memory index.
func (e *Engine) recoverIndex() {
	slog.Info("Starting index recovery from disk...", "totalPages", e.pool.fm.TotalPages)
	var i uint64
	for i = 1; i <= e.pool.fm.TotalPages; i++ {
		p := NewPage(i)
		if err := e.pool.fm.ReadPage(i, p); err != nil {
			slog.Warn("Skipping page during recovery", "pageID", i, "error", err)
			continue
		}
		for s := uint16(0); s < p.Header.SlotCount; s++ {
			rec, err := p.GetRecord(s)
			if err == nil && (rec.Flags == FlagHead || rec.Flags == FlagHeadSingle) {
				e.index[string(rec.Key)] = Location{PageID: i, SlotID: s}
			}
		}
	}
	slog.Info("Index recovery complete", "totalKeys", len(e.index))
}

// insertChunk finds the active page with space and inserts a record chunk.
// Must be called while holding e.mu.Lock().
func (e *Engine) insertChunk(rec *LinkedRecord) (Location, error) {
	for {
		cPage, err := e.pool.FetchPage(e.activePageID)
		if err != nil {
			return Location{}, err
		}
		slotID, err := cPage.Page.InsertRecord(rec)
		if errors.Is(err, ErrPageFull) {
			e.pool.Unpin(e.activePageID, false)
			e.activePageID++
			continue
		} else if err != nil {
			e.pool.Unpin(e.activePageID, false)
			return Location{}, err
		}
		e.pool.Unpin(e.activePageID, true)
		return Location{PageID: e.activePageID, SlotID: slotID}, nil
	}
}

// deleteChain follows the linked list of chunks and tombstones each one.
// Must be called while holding e.mu.Lock().
func (e *Engine) deleteChain(startLoc Location) {
	curr := startLoc
	for curr.PageID != 0 {
		cPage, err := e.pool.FetchPage(curr.PageID)
		if err != nil {
			break
		}
		rec, _ := cPage.Page.GetRecord(curr.SlotID)
		cPage.Page.DeleteRecord(curr.SlotID)
		e.pool.Unpin(curr.PageID, true)
		if rec == nil || rec.Flags == FlagTail || rec.Flags == FlagHeadSingle {
			break
		}
		curr = rec.NextLoc
	}
}

// viewLocked reads and reassembles a value from its chunk chain.
// Must be called while holding at least e.mu.RLock().
func (e *Engine) viewLocked(startLoc Location) ([]byte, error) {
	var result []byte
	curr := startLoc
	for curr.PageID != 0 {
		cPage, err := e.pool.FetchPage(curr.PageID)
		if err != nil {
			return nil, err
		}
		rec, err := cPage.Page.GetRecord(curr.SlotID)
		e.pool.Unpin(curr.PageID, false)
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

// upsertLocked writes key/value as a linked chain of chunks.
// Must be called while holding e.mu.Lock().
func (e *Engine) upsertLocked(key string, value []byte) error {
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
		if totalChunks == 1 {
			rec.Flags = FlagHeadSingle
			rec.Key = []byte(key)
		} else if i == totalChunks-1 {
			rec.Flags = FlagTail
		} else if i == 0 {
			rec.Flags = FlagHead
			rec.Key = []byte(key)
		} else {
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

// -- Public Thread-Safe CRUD Operations --

// Upsert inserts or replaces the value for the given key.
func (e *Engine) Upsert(key string, value []byte) error {
	if len(key) > MaxKeySize {
		return ErrPayloadTooBig
	}
	if value == nil {
		value = []byte{}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(OpUpsert, key, value); err != nil {
		return err
	}
	e.totalOps.Add(1)
	return e.upsertLocked(key, value)
}

// View retrieves the value for the given key.
func (e *Engine) View(key string) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	loc, exists := e.index[key]
	if !exists {
		return nil, ErrNotFound
	}
	return e.viewLocked(loc)
}

// Delete removes the key and tombstones its chunk chain.
func (e *Engine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(OpDelete, key, nil); err != nil {
		return err
	}
	e.totalOps.Add(1)

	if loc, exists := e.index[key]; exists {
		e.deleteChain(loc)
		delete(e.index, key)
		return nil
	}
	return ErrNotFound
}

// Incr atomically reads, increments by delta, and writes back a 64-bit integer value.
// FIX: The previous implementation called View() then Upsert() separately, each acquiring
// their own lock, resulting in a non-atomic read-modify-write and lost update race condition.
// Now the entire operation is performed under a single e.mu.Lock().
func (e *Engine) Incr(key string, delta int64) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var current int64
	if loc, exists := e.index[key]; exists {
		val, err := e.viewLocked(loc)
		if err == nil && len(val) >= 8 {
			current = int64(binary.LittleEndian.Uint64(val))
		}
	}

	newVal := current + delta
	newBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(newBytes, uint64(newVal))

	if err := e.wal.Append(OpIncr, key, newBytes); err != nil {
		return 0, err
	}
	e.totalOps.Add(1)

	return newVal, e.upsertLocked(key, newBytes)
}

// ==========================================
// 7. JANITOR & SHADOW DEFRAGMENTATION
// ==========================================

// StartJanitor runs the background maintenance loop until ctx is cancelled.
// The janitor's WaitGroup entry is managed by the caller (main) for clean shutdown.
func (e *Engine) StartJanitor(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(JanitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("Janitor: context cancelled, stopping.")
			return
		case <-ticker.C:
			e.pool.FlushAll()

			if e.totalOps.Load() > WALCheckpointOps {
				// Safe checkpoint order:
				// 1. All dirty pages are already flushed above.
				// 2. Fsync the data file to guarantee durability.
				// 3. Only then truncate the WAL.
				if err := e.pool.fm.file.Sync(); err != nil {
					slog.Error("Janitor: fsync failed before checkpoint", "error", err)
					continue
				}
				if err := e.wal.Checkpoint(); err != nil {
					slog.Error("Janitor: WAL checkpoint failed", "error", err)
				} else {
					e.totalOps.Store(0)
					slog.Info("Janitor: WAL checkpoint complete.")
				}
			}

			if e.pool.fm.TotalPages > DefragPageThreshold {
				go e.runDefrag()
			}
		}
	}
}

// runDefrag performs shadow defragmentation: copies all live data to a new file,
// then atomically swaps it with the current data file.
func (e *Engine) runDefrag() {
	slog.Info("Starting shadow defragmentation...")
	tmpPath := e.dataPath + ".tmp"

	tmpFm, err := NewFileManager(tmpPath)
	if err != nil {
		slog.Error("Defrag: failed to create temp file manager", "error", err)
		return
	}
	tmpBp := NewBufferPool(tmpFm, 100)
	tmpEngine := &Engine{pool: tmpBp, index: make(map[string]Location), activePageID: 1}

	// Snapshot phase: read all live data under a read lock.
	e.mu.RLock()
	for key, startLoc := range e.index {
		cPage, err := e.pool.FetchPage(startLoc.PageID)
		if err != nil {
			continue
		}
		rec, err := cPage.Page.GetRecord(startLoc.SlotID)
		e.pool.Unpin(startLoc.PageID, false)
		if err != nil || len(rec.Key) == 0 {
			continue
		}
		// Use viewLocked with the current engine's lock already held.
		val, err := e.viewLocked(startLoc)
		if err != nil {
			slog.Warn("Defrag: failed to read value during snapshot", "key", key, "error", err)
			continue
		}
		// tmpEngine has no concurrent writers, so direct upsertLocked is safe.
		if err := tmpEngine.upsertLocked(key, val); err != nil {
			slog.Warn("Defrag: failed to write value to temp engine", "key", key, "error", err)
		}
	}
	e.mu.RUnlock()

	// Swap phase: acquire write lock to finalize the swap.
	e.mu.Lock()
	defer e.mu.Unlock()

	tmpBp.FlushAll()
	if err := tmpFm.file.Sync(); err != nil {
		slog.Error("Defrag: failed to sync temp file before swap", "error", err)
		tmpFm.file.Close()
		os.Remove(tmpPath)
		return
	}
	tmpFm.file.Close()
	e.pool.fm.file.Close()

	if err := os.Rename(tmpPath, e.dataPath); err != nil {
		slog.Error("Defrag: atomic rename failed", "error", err)
		// Re-open the original file to recover.
		newFm, _ := NewFileManager(e.dataPath)
		e.pool.fm = newFm
		return
	}

	newFm, err := NewFileManager(e.dataPath)
	if err != nil {
		slog.Error("Defrag: failed to re-open data file after swap", "error", err)
		return
	}
	e.pool.fm = newFm
	e.activePageID = tmpEngine.activePageID
	if err := e.wal.Checkpoint(); err != nil {
		slog.Error("Defrag: WAL checkpoint after defrag failed", "error", err)
	}
	slog.Info("Shadow defragmentation complete. Storage compacted.")
}

// ==========================================
// 8. TCP LAYER & PROTOCOL
// ==========================================

// handleConnection processes the binary protocol for a single TCP connection.
// Protocol (request):  Magic(2) + Op(1) + ReqID(4) + KeyLen(1) + ValLen(4) + Key + Val
// Protocol (response): Magic(2) + ReqID(4) + Status(1) + DataLen(4) + Data
func handleConnection(conn net.Conn, engine *Engine) {
	defer conn.Close()
	headerBuf := make([]byte, 12)
	for {
		// Set a read deadline to prevent zombie connections.
		if err := conn.SetDeadline(time.Now().Add(ConnReadDeadline)); err != nil {
			return
		}

		if _, err := io.ReadFull(conn, headerBuf); err != nil {
			return
		}

		if binary.LittleEndian.Uint16(headerBuf[0:2]) != MagicBytes {
			return
		}
		op := headerBuf[2]
		reqID := headerBuf[3:7]
		keyLen := int(headerBuf[7])
		valLen := binary.LittleEndian.Uint32(headerBuf[8:12])

		// FIX: Protect against memory exhaustion DoS attack via oversized payload.
		if int(valLen) > MaxValueSize {
			slog.Warn("Rejected oversized payload", "valLen", valLen, "limit", MaxValueSize)
			return
		}

		payloadBuf := make([]byte, keyLen+int(valLen))
		if _, err := io.ReadFull(conn, payloadBuf); err != nil {
			return
		}
		key := string(payloadBuf[:keyLen])
		value := payloadBuf[keyLen:]

		var status byte = StatusOk
		var responseData []byte

		switch op {
		case OpPing:
			// No-op, StatusOk is returned.
		case OpUpsert:
			if err := engine.Upsert(key, value); err != nil {
				slog.Warn("Upsert failed", "key", key, "error", err)
				status = StatusError
			}
		case OpView:
			data, err := engine.View(key)
			if errors.Is(err, ErrNotFound) {
				status = StatusNotFound
			} else if err != nil {
				status = StatusError
			} else {
				responseData = data
			}
		case OpDelete:
			err := engine.Delete(key)
			if errors.Is(err, ErrNotFound) {
				status = StatusNotFound
			} else if err != nil {
				status = StatusError
			}
		case OpIncr:
			if len(value) != 8 {
				status = StatusError
				break
			}
			delta := int64(binary.LittleEndian.Uint64(value))
			newVal, err := engine.Incr(key, delta)
			if err != nil {
				status = StatusError
			} else {
				responseData = make([]byte, 8)
				binary.LittleEndian.PutUint64(responseData, uint64(newVal))
			}
		default:
			status = StatusError
		}

		// Write response: Magic(2) + ReqID(4) + Status(1) + DataLen(4)
		respHeader := make([]byte, 11)
		binary.LittleEndian.PutUint16(respHeader[0:2], MagicBytes)
		copy(respHeader[2:6], reqID)
		respHeader[6] = status
		binary.LittleEndian.PutUint32(respHeader[7:11], uint32(len(responseData)))
		if _, err := conn.Write(respHeader); err != nil {
			return
		}
		if len(responseData) > 0 {
			if _, err := conn.Write(responseData); err != nil {
				return
			}
		}
	}
}

// startTCPServer accepts incoming connections and dispatches them to handleConnection.
// Uses a semaphore channel to enforce MaxConn limit.
func startTCPServer(ctx context.Context, wg *sync.WaitGroup, engine *Engine) {
	listener, err := net.Listen("tcp", Port)
	if err != nil {
		slog.Error("Failed to start TCP listener", "port", Port, "error", err)
		return
	}
	slog.Info("Storage TCP engine started", "port", Port)

	sem := make(chan struct{}, MaxConn)
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Accept returns an error when the listener is closed (ctx cancelled).
			return
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer func() { <-sem }()
			handleConnection(c, engine)
		}(conn)
	}
}

// ==========================================
// 9. MAIN ENTRY & GRACEFUL SHUTDOWN
// ==========================================

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	dataPath := "/data/data.bin"
	walPath := "/data/wal.bin"
	if _, err := os.Stat("/data"); os.IsNotExist(err) {
		dataPath = "data.bin"
		walPath = "wal.bin"
		slog.Info("Falling back to local paths for development", "dataPath", dataPath, "walPath", walPath)
	}

	engine, err := NewEngine(dataPath, walPath)
	if err != nil {
		slog.Error("Failed to initialize engine", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	// FIX: Janitor is now tracked by wg for clean shutdown, preventing data races
	// during FlushAll/Checkpoint when the main goroutine also calls them.
	wg.Add(1)
	go engine.StartJanitor(ctx, &wg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		startTCPServer(ctx, &wg, engine)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Termination signal received. Starting graceful shutdown...")
	cancel()  // Signal TCP server and Janitor to stop.
	wg.Wait() // Wait for all goroutines (connections + janitor) to finish.

	// Final flush: ensure all remaining dirty pages are written.
	engine.pool.FlushAll()

	// Safe checkpoint sequence:
	// 1. Flush dirty pages (done above).
	// 2. Fsync the data file (guarantee durability).
	// 3. Checkpoint (truncate) the WAL.
	if err := engine.pool.fm.file.Sync(); err != nil {
		slog.Error("Shutdown: fsync failed", "error", err)
	}
	if err := engine.wal.Checkpoint(); err != nil {
		slog.Error("Shutdown: WAL checkpoint failed", "error", err)
	}
	if err := engine.pool.fm.file.Close(); err != nil {
		slog.Error("Shutdown: failed to close data file", "error", err)
	}
	if err := engine.wal.file.Close(); err != nil {
		slog.Error("Shutdown: failed to close WAL file", "error", err)
	}

	slog.Info("Shutdown complete. Data is safe.")
}
