package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrCorrupt means a segment holds damage that can't be explained by a crash
// interrupting the last write: a bad header, a seq out of order, or a bad
// record with acknowledged data after it. Recovery must stop, and must never
// skip over the damage.
var ErrCorrupt = errors.New("wal: corrupt")

// segmentInfo is what scanSegment learned about a segment.
type segmentInfo struct {
	lastSeq  uint64 // seq of the last good record, or startSeq-1 if there are none
	validLen int    // bytes from the start of the file through the last good record
	tornLen  int    // bytes after validLen forming a torn tail; non-zero only for the last segment
}

// scanSegment validates the bytes of one segment file and calls apply for each
// good record, in order. startSeq comes from the file name and must match the
// header. A Record passed to apply points into data and is valid only during
// the call.
//
// A bad record (truncated or invalid) at offset X is classified as follows:
//   - In a sealed segment (last == false), it is always ErrCorrupt: nothing
//     was being written to that file when the crash happened.
//   - In the last segment, it is a torn tail if no valid record follows it
//     anywhere in the file (see findLaterRecord). Then scanning stops at X, and
//     the caller truncates the file there. Otherwise it is ErrCorrupt.
//
// The rule rests on an invariant of Writer.Append: each record is synced
// before the next is written, so a crash can damage only the final record.
func scanSegment(name string, data []byte, startSeq uint64, last bool, apply func(Record) error) (segmentInfo, error) {
	if err := checkSegmentHeader(data, startSeq); err != nil {
		return segmentInfo{}, fmt.Errorf("%w: %s: %w", ErrCorrupt, name, err)
	}
	off := segmentHeaderSize
	expected := startSeq
	for off < len(data) {
		r, n, err := DecodeRecord(data[off:])
		if err != nil {
			if !last {
				return segmentInfo{}, fmt.Errorf("%w: %s: offset %d, sealed segment, after seq %d: %w",
					ErrCorrupt, name, off, expected-1, err)
			}
			if laterOff, ok := findLaterRecord(data, off+1, expected); ok {
				return segmentInfo{}, fmt.Errorf("%w: %s: bad record at offset %d (after seq %d), but a valid record follows at offset %d: %w",
					ErrCorrupt, name, off, expected-1, laterOff, err)
			}
			return segmentInfo{lastSeq: expected - 1, validLen: off, tornLen: len(data) - off}, nil
		}
		// A checksum-valid record with the wrong seq can't come from a torn
		// write. It was written whole, so the log itself is out of order.
		if r.Seq != expected {
			return segmentInfo{}, fmt.Errorf("%w: %s: offset %d: seq %d, want %d", ErrCorrupt, name, off, r.Seq, expected)
		}
		if err := apply(r); err != nil {
			return segmentInfo{}, err
		}
		off += n
		expected++
	}
	return segmentInfo{lastSeq: expected - 1, validLen: off}, nil
}

func checkSegmentHeader(data []byte, startSeq uint64) error {
	// CreateSegment syncs the header before the file gets its name, so even
	// the last segment must have a whole header. A short one is damage, not a
	// torn write.
	if len(data) < segmentHeaderSize {
		return fmt.Errorf("file is %d bytes, header needs %d", len(data), segmentHeaderSize)
	}
	if magic := binary.LittleEndian.Uint32(data[0:4]); magic != segmentMagic {
		return fmt.Errorf("bad magic %08x", magic)
	}
	if v := binary.LittleEndian.Uint16(data[4:6]); v != segmentVersion {
		return fmt.Errorf("unsupported version %d", v)
	}
	if reserved := binary.LittleEndian.Uint16(data[6:8]); reserved != 0 {
		return fmt.Errorf("reserved header field is %d, want 0", reserved)
	}
	if got := binary.LittleEndian.Uint64(data[8:16]); got != startSeq {
		return fmt.Errorf("header startSeq %d doesn't match file name (%d)", got, startSeq)
	}
	return nil
}

// findLaterRecord reports whether any offset from `from` onward holds a valid
// record with seq expected or expected+1. Such a record means the damage
// isn't at the tail: acknowledged data follows it.
//
// expected+1 is the normal case: the record after the damaged one. expected
// covers damage that sits before the record with the next seq, e.g. garbage
// overwriting the end of the previous record.
//
// Two cheap checks (length in range, seq matches) run before any CRC, so
// almost every offset is rejected without checksumming anything. The CRC
// still decides, so a false positive needs a 2^-32 coincidence, or
// attacker-chosen value bytes (see edge case 1.31).
func findLaterRecord(data []byte, from int, expected uint64) (int, bool) {
	for i := from; i+headerSize <= len(data); i++ {
		if binary.LittleEndian.Uint32(data[i:i+4]) > MaxPayload {
			continue
		}
		if seq := binary.LittleEndian.Uint64(data[i+8 : i+16]); seq != expected && seq != expected+1 {
			continue
		}
		if _, _, err := DecodeRecord(data[i:]); err == nil {
			return i, true
		}
	}
	return 0, false
}
