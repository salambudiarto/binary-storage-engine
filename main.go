package main

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"hash/fnv"
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
// 1. KONSTANTA & KONFIGURASI SISTEM
// ==========================================
const (
	PageSize       = 4096
	PageHeaderSize = 32
	MaxChunkSize   = 64
	MaxKeySize     = 64
	SlotSize       = 4
	PoolSizeMB     = 64
	MaxCachePages  = (PoolSizeMB * 1024 * 1024) / PageSize // 16384 pages

	Port    = ":7070"
	MaxConn = 100

	MagicBytes  uint16 = 0xBE57
	GlobalMagic        = "BSENGINE"
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
	ErrPageFull      = errors.New("engine: page full")
	ErrNotFound      = errors.New("engine: key not found")
	ErrInvalidRecord = errors.New("engine: invalid record")
	ErrSlotNotFound  = errors.New("engine: slot not found")
	ErrPayloadTooBig = errors.New("engine: key exceeds limits")
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

func NewPage(pageID uint64) *Page {
	p := &Page{}
	p.Header.PageID = pageID
	p.Header.PageType = 1
	p.Header.LowerOffset = PageHeaderSize
	p.Header.UpperOffset = PageSize
	p.SyncHeaderToBuffer()
	return p
}

func (p *Page) SyncHeaderToBuffer() {
	binary.LittleEndian.PutUint64(p.Buffer[0:8], p.Header.PageID)
	binary.LittleEndian.PutUint16(p.Buffer[8:10], p.Header.PageType)
	binary.LittleEndian.PutUint16(p.Buffer[10:12], p.Header.SlotCount)
	binary.LittleEndian.PutUint16(p.Buffer[12:14], p.Header.LowerOffset)
	binary.LittleEndian.PutUint16(p.Buffer[14:16], p.Header.UpperOffset)
	binary.LittleEndian.PutUint16(p.Buffer[16:18], p.Header.FragBytes)
	p.Header.Checksum = crc32.ChecksumIEEE(p.Buffer[0:18])
	binary.LittleEndian.PutUint32(p.Buffer[18:22], p.Header.Checksum)
}

func EncodeRecord(rec *LinkedRecord) ([]byte, error) {
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

func DecodeRecord(data []byte) (*LinkedRecord, error) {
	if len(data) < 12 {
		return nil, ErrInvalidRecord
	}
	rec := &LinkedRecord{Flags: data[0]}
	rec.NextLoc.PageID = binary.LittleEndian.Uint64(data[1:9])
	rec.NextLoc.SlotID = binary.LittleEndian.Uint16(data[9:11])

	offset := 11
	isHead := rec.Flags == FlagHead || rec.Flags == FlagHeadSingle
	if isHead {
		keyLen := int(data[offset])
		offset++
		rec.Key = make([]byte, keyLen)
		copy(rec.Key, data[offset:offset+keyLen])
		offset += keyLen
	}
	dataLen := int(data[offset])
	offset++
	rec.Data = make([]byte, dataLen)
	copy(rec.Data, data[offset:offset+dataLen])
	return rec, nil
}

func (p *Page) InsertRecord(rec *LinkedRecord) (uint16, error) {
	encoded, _ := EncodeRecord(rec)
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
	p.SyncHeaderToBuffer()
	return slotID, nil
}

func (p *Page) GetRecord(slotID uint16) (*LinkedRecord, error) {
	if slotID >= p.Header.SlotCount {
		return nil, ErrSlotNotFound
	}
	slotPos := PageHeaderSize + (slotID * SlotSize)
	recOffset := binary.LittleEndian.Uint16(p.Buffer[slotPos : slotPos+2])
	recLen := binary.LittleEndian.Uint16(p.Buffer[slotPos+2 : slotPos+4])
	if recOffset == 0 || p.Buffer[recOffset] == FlagTombstone {
		return nil, ErrSlotNotFound
	}
	return DecodeRecord(p.Buffer[recOffset : recOffset+recLen])
}

func (p *Page) DeleteRecord(slotID uint16) {
	slotPos := PageHeaderSize + (slotID * SlotSize)
	recOffset := binary.LittleEndian.Uint16(p.Buffer[slotPos : slotPos+2])
	recLen := binary.LittleEndian.Uint16(p.Buffer[slotPos+2 : slotPos+4])
	if recOffset != 0 && p.Buffer[recOffset] != FlagTombstone {
		p.Buffer[recOffset] = FlagTombstone
		p.Header.FragBytes += recLen + SlotSize
		p.SyncHeaderToBuffer()
	}
}

// ==========================================
// 3. FILE MANAGER & GLOBAL HEADER
// ==========================================
type FileManager struct {
	file       *os.File
	TotalPages uint64
}

func NewFileManager(path string) (*FileManager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	fm := &FileManager{file: file}
	info, _ := file.Stat()

	// Jika file baru, inisialisasi Global Header (Page 0)
	if info.Size() == 0 {
		fm.TotalPages = 0
		fm.WriteGlobalHeader()
	} else {
		fm.ReadGlobalHeader()
	}
	return fm, nil
}

func (fm *FileManager) WriteGlobalHeader() {
	buf := make([]byte, PageSize)
	copy(buf[0:8], GlobalMagic)
	binary.LittleEndian.PutUint16(buf[8:10], 1) // Version
	binary.LittleEndian.PutUint16(buf[10:12], PageSize)
	binary.LittleEndian.PutUint64(buf[12:20], fm.TotalPages)
	fm.file.WriteAt(buf, 0) // Page 0
}

func (fm *FileManager) ReadGlobalHeader() {
	buf := make([]byte, 64)
	fm.file.ReadAt(buf, 0)
	if string(buf[0:8]) == GlobalMagic {
		fm.TotalPages = binary.LittleEndian.Uint64(buf[12:20])
	}
}

func (fm *FileManager) ReadPage(id uint64, p *Page) error {
	_, err := fm.file.ReadAt(p.Buffer[:], int64(id)*PageSize)
	if err == nil {
		p.Header.PageID = binary.LittleEndian.Uint64(p.Buffer[0:8])
		p.Header.SlotCount = binary.LittleEndian.Uint16(p.Buffer[10:12])
		p.Header.LowerOffset = binary.LittleEndian.Uint16(p.Buffer[12:14])
		p.Header.UpperOffset = binary.LittleEndian.Uint16(p.Buffer[14:16])
		p.Header.FragBytes = binary.LittleEndian.Uint16(p.Buffer[16:18])
	}
	return err
}

func (fm *FileManager) WritePage(id uint64, p *Page) error {
	p.SyncHeaderToBuffer()
	_, err := fm.file.WriteAt(p.Buffer[:], int64(id)*PageSize)
	if id > fm.TotalPages {
		fm.TotalPages = id
		fm.WriteGlobalHeader() // Update total pages di header
	}
	return err
}

// ==========================================
// 4. WRITE-AHEAD LOG (WAL)
// ==========================================
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func NewWAL(path string) *WAL {
	f, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	return &WAL{file: f}
}

func (w *WAL) Append(op byte, key string, val []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// LSN(8) + Op(1) + KeyLen(2) + ValLen(4) + Key + Val + Checksum(4)
	buf := make([]byte, 15+len(key)+len(val)+4)
	buf[8] = op
	binary.LittleEndian.PutUint16(buf[9:11], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[11:15], uint32(len(val)))
	copy(buf[15:], key)
	copy(buf[15+len(key):], val)
	w.file.Write(buf)
	w.file.Sync() // fsync untuk crash safety
}

func (w *WAL) Checkpoint() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.file.Truncate(0)
	w.file.Seek(0, 0)
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

func NewBufferPool(fm *FileManager, cap int) *BufferPool {
	return &BufferPool{fm: fm, pages: make(map[uint64]*list.Element), lru: list.New(), capacity: cap}
}

func (bp *BufferPool) FetchPage(id uint64) (*CachedPage, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if elem, ok := bp.pages[id]; ok {
		bp.lru.MoveToFront(elem)
		cPage := elem.Value.(*CachedPage)
		cPage.PinCount++
		return cPage, nil
	}
	if len(bp.pages) >= bp.capacity {
		for e := bp.lru.Back(); e != nil; e = e.Prev() {
			cPage := e.Value.(*CachedPage)
			if cPage.PinCount == 0 {
				if cPage.Dirty {
					bp.fm.WritePage(cPage.Page.Header.PageID, cPage.Page)
				}
				delete(bp.pages, cPage.Page.Header.PageID)
				bp.lru.Remove(e)
				break
			}
		}
	}
	p := NewPage(id)
	if err := bp.fm.ReadPage(id, p); err != nil && err.Error() != "EOF" {
		return nil, err
	}
	cPage := &CachedPage{Page: p, PinCount: 1}
	bp.pages[id] = bp.lru.PushFront(cPage)
	return cPage, nil
}

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

func (bp *BufferPool) FlushAll() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	for e := bp.lru.Front(); e != nil; e = e.Next() {
		cPage := e.Value.(*CachedPage)
		if cPage.Dirty {
			bp.fm.WritePage(cPage.Page.Header.PageID, cPage.Page)
			cPage.Dirty = false
		}
	}
}

// ==========================================
// 6. STORAGE ENGINE CORE
// ==========================================
type Engine struct {
	mu           sync.RWMutex
	pool         *BufferPool
	wal          *WAL
	index        map[uint64]Location
	activePageID uint64
	totalOps     uint64
	dataPath     string
}

func NewEngine(dataPath string, walPath string) *Engine {
	fm, _ := NewFileManager(dataPath)
	bp := NewBufferPool(fm, MaxCachePages)
	wal := NewWAL(walPath)

	e := &Engine{
		pool:         bp,
		wal:          wal,
		index:        make(map[uint64]Location),
		activePageID: fm.TotalPages,
		dataPath:     dataPath,
	}

	if e.activePageID == 0 {
		e.activePageID = 1
	} // Page 0 is Header
	e.RecoverIndex()
	return e
}

func HashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// RecoverIndex melakukan Scan penuh pada Disk saat Startup
func (e *Engine) RecoverIndex() {
	slog.Info("Memulai Index Recovery dari Disk...", "TotalPages", e.pool.fm.TotalPages)
	var i uint64
	for i = 1; i <= e.pool.fm.TotalPages; i++ {
		p := NewPage(i)
		if err := e.pool.fm.ReadPage(i, p); err == nil {
			for s := uint16(0); s < p.Header.SlotCount; s++ {
				rec, err := p.GetRecord(s)
				if err == nil && (rec.Flags == FlagHead || rec.Flags == FlagHeadSingle) {
					e.index[HashKey(string(rec.Key))] = Location{PageID: i, SlotID: s}
				}
			}
		}
	}
	slog.Info("Index Recovery Selesai", "TotalKeys", len(e.index))
}

func (e *Engine) insertChunk(rec *LinkedRecord) (Location, error) {
	for {
		cPage, err := e.pool.FetchPage(e.activePageID)
		if err != nil {
			return Location{}, err
		}
		slotID, err := cPage.Page.InsertRecord(rec)
		if err == ErrPageFull {
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
		if rec == nil {
			break
		}
		curr = rec.NextLoc
	}
}

// -- Operasi CRUD (Thread-Safe) --

func (e *Engine) Upsert(key string, value []byte) error {
	if len(key) > MaxKeySize {
		return ErrPayloadTooBig
	}
	if len(value) == 0 {
		value = []byte{}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.wal.Append(OpUpsert, key, value) // Crash Safety
	atomic.AddUint64(&e.totalOps, 1)

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

	hash := HashKey(key)
	if oldLoc, exists := e.index[hash]; exists {
		e.deleteChain(oldLoc)
	}
	e.index[hash] = nextLoc
	return nil
}

func (e *Engine) View(key string) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	loc, exists := e.index[HashKey(key)]
	if !exists {
		return nil, ErrNotFound
	}

	var result []byte
	curr := loc
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

func (e *Engine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.wal.Append(OpDelete, key, nil)
	atomic.AddUint64(&e.totalOps, 1)

	hash := HashKey(key)
	if loc, exists := e.index[hash]; exists {
		e.deleteChain(loc)
		delete(e.index, hash)
		return nil
	}
	return ErrNotFound
}

func (e *Engine) Incr(key string, delta int64) (int64, error) {
	// Panggil View langsung (View sudah memiliki e.mu.RLock() di dalamnya)
	valBytes, err := e.View(key)

	var current int64 = 0
	if err == nil && len(valBytes) >= 8 {
		current = int64(binary.LittleEndian.Uint64(valBytes))
	}

	// Lakukan kalkulasi penambahan (Increment)
	newVal := current + delta
	newBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(newBytes, uint64(newVal))

	// Panggil Upsert langsung (Upsert sudah memiliki e.mu.Lock() di dalamnya)
	err = e.Upsert(key, newBytes)

	return newVal, err
}

// ==========================================
// 7. JANITOR & SHADOW DEFRAGMENTATION
// ==========================================
func (e *Engine) StartJanitor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.pool.FlushAll()

			// WAL Checkpoint jika operasi melebihi threshold
			if atomic.LoadUint64(&e.totalOps) > 10000 {
				e.wal.Checkpoint()
				atomic.StoreUint64(&e.totalOps, 0)
			}

			// Cek Fragmentasi (Simulasi ringan: Jika total page > 1000 dan dirasa bloated)
			// Dalam produksi ideal, Anda melacak total FragBytes vs Total Space.
			if e.pool.fm.TotalPages > 500 {
				// go e.runDefrag() // Non-blocking
			}
		}
	}
}

// Shadow Defragmentation: Tulis ulang data live ke file baru
func (e *Engine) runDefrag() {
	slog.Info("Memulai Shadow Defragmentation...")
	tmpPath := e.dataPath + ".tmp"
	tmpFm, _ := NewFileManager(tmpPath)
	tmpBp := NewBufferPool(tmpFm, 100) // Cache kecil untuk defrag
	tmpEngine := &Engine{pool: tmpBp, activePageID: 1}

	e.mu.RLock() // Lock baca selama proses copy untuk konsistensi snapshot
	for keyHash, startLoc := range e.index {
		// Rekonstruksi key asli dari rantai (Kita harus trace balik key, tapi HashIndex menyulitkan.)
		// OPTIMALISASI: Dalam arsitektur hash murni, kita baca Head Record untuk dapat `rec.Key`.
		cPage, _ := e.pool.FetchPage(startLoc.PageID)
		rec, _ := cPage.Page.GetRecord(startLoc.SlotID)
		e.pool.Unpin(startLoc.PageID, false)

		if rec != nil && len(rec.Key) > 0 {
			val, _ := e.View(string(rec.Key))
			tmpEngine.Upsert(string(rec.Key), val) // Tulis rapat ke tmp
			_ = keyHash                            // Keep compiler happy
		}
	}
	e.mu.RUnlock()

	// Fase Swap (Write Lock sebentar)
	e.mu.Lock()
	defer e.mu.Unlock()
	tmpBp.FlushAll()
	tmpFm.file.Close()
	e.pool.fm.file.Close()

	os.Rename(tmpPath, e.dataPath) // Atomic OS level swap

	// Re-init pointer engine
	newFm, _ := NewFileManager(e.dataPath)
	e.pool.fm = newFm
	e.activePageID = tmpEngine.activePageID
	e.wal.Checkpoint()
	slog.Info("Defragmentasi Selesai. File Storage kembali rapat.")
}

// ==========================================
// 8. TCP LAYER & PROTOCOL
// ==========================================
func handleConnection(conn net.Conn, engine *Engine) {
	defer conn.Close()
	headerBuf := make([]byte, 12)
	for {
		_, err := io.ReadFull(conn, headerBuf)
		if err != nil {
			return
		}

		if binary.LittleEndian.Uint16(headerBuf[0:2]) != MagicBytes {
			return
		}
		op := headerBuf[2]
		reqID := headerBuf[3:7]
		keyLen := int(headerBuf[7])
		valLen := binary.LittleEndian.Uint32(headerBuf[8:12])

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
			// Ping OK
		case OpUpsert:
			if err := engine.Upsert(key, value); err != nil {
				status = StatusError
			}
		case OpView:
			data, err := engine.View(key)
			if err == ErrNotFound {
				status = StatusNotFound
			} else if err != nil {
				status = StatusError
			} else {
				responseData = data
			}
		case OpDelete:
			if err := engine.Delete(key); err == ErrNotFound {
				status = StatusNotFound
			} else if err != nil {
				status = StatusError
			}
		case OpIncr:
			if len(value) == 8 {
				delta := int64(binary.LittleEndian.Uint64(value))
				newVal, err := engine.Incr(key, delta)
				if err != nil {
					status = StatusError
				} else {
					responseData = make([]byte, 8)
					binary.LittleEndian.PutUint64(responseData, uint64(newVal))
				}
			} else {
				status = StatusError
			}
		default:
			status = StatusError
		}

		respHeader := make([]byte, 11)
		binary.LittleEndian.PutUint16(respHeader[0:2], MagicBytes)
		copy(respHeader[2:6], reqID)
		respHeader[6] = status
		binary.LittleEndian.PutUint32(respHeader[7:11], uint32(len(responseData)))
		conn.Write(respHeader)
		if len(responseData) > 0 {
			conn.Write(responseData)
		}
	}
}

func startTCPServer(ctx context.Context, wg *sync.WaitGroup, engine *Engine) {
	listener, err := net.Listen("tcp", Port)
	if err != nil {
		os.Exit(1)
	}
	slog.Info("Storage TCP Engine Berjalan", "port", Port)

	sem := make(chan struct{}, MaxConn)
	go func() { <-ctx.Done(); listener.Close() }()

	for {
		conn, err := listener.Accept()
		if err != nil {
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
		walPath = "wal.bin" // Fallback local testing
	}

	engine := NewEngine(dataPath, walPath)
	ctx, cancel := context.WithCancel(context.Background())

	// Start Background Janitor
	go engine.StartJanitor(ctx)

	var wg sync.WaitGroup
	go startTCPServer(ctx, &wg, engine)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Sinyal Terminasi Diterima. Memulai Graceful Shutdown...")
	cancel() // Stop TCP & Janitor
	wg.Wait()

	engine.pool.FlushAll()
	engine.wal.Checkpoint()
	engine.pool.fm.file.Sync()
	engine.pool.fm.file.Close()
	engine.wal.file.Close()

	slog.Info("Shutdown Selesai. Data Aman.")
	os.Exit(0)
}
