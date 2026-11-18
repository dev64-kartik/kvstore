// Package wal implements the Phase 1 write-ahead log. It is retired in Phase 3,
// when the Raft log takes over durability.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"

	"github.com/dev64-kartik/kvstore/internal/store"
)

// Record layout. All integers are little-endian.
//
//	offset 0   length u32   payload length in bytes
//	offset 4   crc    u32   CRC32C over the length field, seq and payload
//	offset 8   seq    u64
//	offset 16  payload      op u8 | keyLen uvarint | key | valLen uvarint | value
//
// The value part (valLen + value) is present only for OpPut.
const headerSize = 16

// Size limits, from the data model. They are enforced on both encode and decode, so a record that
// could not be read back can never be written.
const (
	MaxKeySize   = store.MaxKeySize
	MaxValueSize = store.MaxValueSize

	// MaxPayload bounds the length field: op byte, two length varints, key,
	// value. Anything larger can only be garbage.
	MaxPayload = 1 + binary.MaxVarintLen32 + MaxKeySize + binary.MaxVarintLen32 + MaxValueSize
)

// Op is the kind of mutation a record describes. Zero is deliberately not a
// valid Op, so an all-zero payload never decodes as a real operation.
type Op uint8

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

// Record is one logged mutation.
type Record struct {
	Seq   uint64
	Op    Op
	Key   []byte
	Value []byte // always empty for OpDelete
}

var (
	// ErrTruncated: the buffer ends before the record does.
	ErrTruncated = errors.New("wal: truncated record")
	// ErrBadRecord: the bytes are present but don't form a valid record (bad
	// checksum, impossible length, malformed payload).
	ErrBadRecord = errors.New("wal: bad record")
	// ErrInvalidRecord: the caller asked to encode a record that breaks the
	// format's rules (unknown op, empty or oversized key or value).
	ErrInvalidRecord = errors.New("wal: invalid record")
)

// castagnoli selects CRC32C, which modern CPUs compute in hardware. It is set
// once at package load and only ever read.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// AppendRecord appends the encoding of r to dst and returns the extended
// slice, in the style of the stdlib's append functions, so the writer can
// reuse one buffer. On error, dst is returned unchanged.
func AppendRecord(dst []byte, r Record) ([]byte, error) {
	if err := validate(r); err != nil {
		return dst, fmt.Errorf("%w: %w", ErrInvalidRecord, err)
	}
	start := len(dst)
	// Reserve the header. Its bytes may be stale, so all 16 are overwritten below.
	dst = slices.Grow(dst, headerSize)[:start+headerSize]

	dst = append(dst, byte(r.Op))
	dst = binary.AppendUvarint(dst, uint64(len(r.Key)))
	dst = append(dst, r.Key...)
	if r.Op == OpPut {
		dst = binary.AppendUvarint(dst, uint64(len(r.Value)))
		dst = append(dst, r.Value...)
	}

	rec := dst[start:]
	binary.LittleEndian.PutUint32(rec[0:4], uint32(len(rec)-headerSize))
	binary.LittleEndian.PutUint64(rec[8:16], r.Seq)
	binary.LittleEndian.PutUint32(rec[4:8], checksum(rec)) // last: covers the fields above
	return dst, nil
}

// DecodeRecord decodes the record at the start of buf and returns it with the
// number of bytes it occupies.
//
// The returned Key and Value point into buf; they are not copies. They stay
// valid only while buf is unchanged. Recovery passes them straight to
// store.Map.Put, which copies them.
func DecodeRecord(buf []byte) (Record, int, error) {
	if len(buf) < headerSize {
		return Record{}, 0, fmt.Errorf("%w: %d bytes, header needs %d", ErrTruncated, len(buf), headerSize)
	}
	length := binary.LittleEndian.Uint32(buf[0:4])
	// Check the bound before comparing against len(buf). A garbage length such
	// as 3 GB must be reported as a bad record, not as "truncated, the rest
	// hasn't been read yet".
	if length > MaxPayload {
		return Record{}, 0, fmt.Errorf("%w: payload length %d exceeds max %d", ErrBadRecord, length, MaxPayload)
	}
	total := headerSize + int(length)
	if len(buf) < total {
		return Record{}, 0, fmt.Errorf("%w: %d bytes, record needs %d", ErrTruncated, len(buf), total)
	}
	rec := buf[:total]
	if stored, computed := binary.LittleEndian.Uint32(rec[4:8]), checksum(rec); stored != computed {
		return Record{}, 0, fmt.Errorf("%w: checksum mismatch (stored %08x, computed %08x)", ErrBadRecord, stored, computed)
	}
	// A matching checksum means these are the bytes the writer wrote. If the
	// payload is still malformed, the writer had a bug, which is just as fatal.
	r, err := decodePayload(rec[headerSize:])
	if err != nil {
		return Record{}, 0, fmt.Errorf("%w: %w", ErrBadRecord, err)
	}
	r.Seq = binary.LittleEndian.Uint64(rec[8:16])
	return r, total, nil
}

func decodePayload(p []byte) (Record, error) {
	if len(p) == 0 {
		return Record{}, errors.New("empty payload")
	}
	r := Record{Op: Op(p[0])}
	key, p, err := readLengthPrefixed(p[1:])
	if err != nil {
		return Record{}, fmt.Errorf("key: %w", err)
	}
	r.Key = key
	switch r.Op {
	case OpPut:
		if r.Value, p, err = readLengthPrefixed(p); err != nil {
			return Record{}, fmt.Errorf("value: %w", err)
		}
	case OpDelete:
	default:
		return Record{}, fmt.Errorf("unknown op %d", r.Op)
	}
	if len(p) != 0 {
		return Record{}, fmt.Errorf("%d trailing bytes after payload", len(p))
	}
	return r, validate(r)
}

// readLengthPrefixed reads a uvarint length n followed by n bytes, returning
// those bytes (pointing into p) and the rest of p.
func readLengthPrefixed(p []byte) (data, rest []byte, err error) {
	n, k := binary.Uvarint(p)
	if k <= 0 {
		return nil, nil, errors.New("malformed length varint")
	}
	// Only the shortest varint form is accepted, so every valid record has
	// exactly one encoding. Re-encoding a decoded record gives back the same
	// bytes, and the fuzz test checks exactly that.
	if k != uvarintLen(n) {
		return nil, nil, errors.New("non-minimal length varint")
	}
	p = p[k:]
	if n > uint64(len(p)) {
		return nil, nil, fmt.Errorf("length %d exceeds remaining %d bytes", n, len(p))
	}
	// p[:n:n] caps the capacity at n. Without that, a caller appending to the
	// returned slice would overwrite the bytes that follow it in p.
	return p[:n:n], p[n:], nil
}

func uvarintLen(n uint64) int {
	size := 1
	for n >= 0x80 {
		n >>= 7
		size++
	}
	return size
}

// checksum returns the CRC32C of a record's length field, seq and payload,
// skipping bytes 4..8 where the checksum itself is stored.
func checksum(rec []byte) uint32 {
	c := crc32.Update(0, castagnoli, rec[0:4])
	return crc32.Update(c, castagnoli, rec[8:])
}

func validate(r Record) error {
	switch r.Op {
	case OpPut:
	case OpDelete:
		if len(r.Value) != 0 {
			return errors.New("delete carries a value")
		}
	default:
		return fmt.Errorf("unknown op %d", r.Op)
	}
	if len(r.Key) == 0 {
		return errors.New("empty key")
	}
	if len(r.Key) > MaxKeySize {
		return fmt.Errorf("key is %d bytes, max %d", len(r.Key), MaxKeySize)
	}
	if len(r.Value) > MaxValueSize {
		return fmt.Errorf("value is %d bytes, max %d", len(r.Value), MaxValueSize)
	}
	return nil
}
