package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dev64-kartik/kvstore/internal/vfs/crashfs"
	"github.com/dev64-kartik/kvstore/internal/vfs/vfstest"
)

// buildSegment returns a segment file holding n records with seqs startSeq..,
// plus the offset where each record starts.
func buildSegment(t testing.TB, startSeq uint64, n int) (data []byte, offsets []int) {
	t.Helper()
	data = encodeSegmentHeader(startSeq)
	for i := range n {
		offsets = append(offsets, len(data))
		r := Record{Seq: startSeq + uint64(i), Op: OpPut, Key: fmt.Appendf(nil, "key-%d", i), Value: bytes.Repeat([]byte{'v'}, 10+i)}
		var err error
		if data, err = AppendRecord(data, r); err != nil {
			t.Fatal(err)
		}
	}
	return data, offsets
}

// scan runs scanSegment and collects the seqs it applied.
func scan(data []byte, startSeq uint64, last bool) ([]uint64, segmentInfo, error) {
	var seqs []uint64
	info, err := scanSegment("seg", data, startSeq, last, func(r Record) error {
		seqs = append(seqs, r.Seq)
		return nil
	})
	return seqs, info, err
}

func seqRange(from uint64, n int) []uint64 {
	var s []uint64
	for i := range n {
		s = append(s, from+uint64(i))
	}
	return s
}

func TestScanCleanSegment(t *testing.T) {
	for _, n := range []int{0, 1, 5} {
		data, _ := buildSegment(t, 10, n)
		for _, last := range []bool{false, true} {
			seqs, info, err := scan(data, 10, last)
			if err != nil || !slices.Equal(seqs, seqRange(10, n)) {
				t.Fatalf("n=%d last=%v: seqs %v, err %v", n, last, seqs, err)
			}
			if want := (segmentInfo{lastSeq: 10 + uint64(n) - 1, validLen: len(data)}); info != want {
				t.Fatalf("n=%d last=%v: info %+v, want %+v", n, last, info, want)
			}
		}
	}
}

func TestScanBadHeaderIsCorrupt(t *testing.T) {
	good, _ := buildSegment(t, 1, 1)
	patch := func(off int, b ...byte) []byte {
		d := bytes.Clone(good)
		copy(d[off:], b)
		return d
	}
	tests := map[string][]byte{
		"empty file":        {},
		"short header":      good[:segmentHeaderSize-1],
		"bad magic":         patch(0, 'X'),
		"future version":    patch(4, 2),
		"reserved set":      patch(6, 1),
		"startSeq mismatch": patch(8, 2), // header says 2, file name says 1
	}
	for name, data := range tests {
		for _, last := range []bool{false, true} {
			// last=true matters most: a bad header must never be mistaken for
			// a torn write and "repaired".
			if _, _, err := scan(data, 1, last); !errors.Is(err, ErrCorrupt) {
				t.Errorf("%s (last=%v): err = %v, want ErrCorrupt", name, last, err)
			}
		}
	}
}

// Records that pass their checksum but carry the wrong seq were written
// whole, so this is corruption even in the last segment.
func TestScanSeqOutOfOrderIsCorrupt(t *testing.T) {
	tests := map[string][]uint64{
		"first seq wrong": {2, 3},
		"gap":             {1, 3},
		"duplicate":       {1, 1},
		"regression":      {1, 2, 1},
	}
	for name, seqs := range tests {
		data := encodeSegmentHeader(1)
		for _, s := range seqs {
			data = append(data, mustEncode(t, Record{Seq: s, Op: OpDelete, Key: []byte("k")})...)
		}
		if _, _, err := scan(data, 1, true); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
}

// Cut the final record at every possible byte: always a torn tail in the last
// segment, always corruption in a sealed one.
func TestScanTornTailAtEveryCut(t *testing.T) {
	full, offsets := buildSegment(t, 1, 4)
	lastStart := offsets[3]
	for cut := lastStart + 1; cut < len(full); cut++ {
		data := full[:cut]
		seqs, info, err := scan(data, 1, true)
		want := segmentInfo{lastSeq: 3, validLen: lastStart, tornLen: cut - lastStart}
		if err != nil || info != want || !slices.Equal(seqs, seqRange(1, 3)) {
			t.Fatalf("cut %d: seqs %v, info %+v, err %v; want 1..3, %+v", cut, seqs, info, err, want)
		}
		if _, _, err := scan(data, 1, false); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("cut %d in sealed segment: err = %v, want ErrCorrupt", cut, err)
		}
	}
}

// Tails a real crash can leave that aren't simple cuts. All must be torn, not
// corrupt.
func TestScanTornTailShapes(t *testing.T) {
	full, offsets := buildSegment(t, 1, 3)
	lastStart := offsets[2]

	zeroSector := bytes.Clone(full) // the "seed 58" case: full length, one zeroed span
	clear(zeroSector[lastStart+5 : lastStart+12])
	zeroTail := append(bytes.Clone(full), make([]byte, 4096)...) // file size grew, data never landed
	flipLast := bytes.Clone(full)
	flipLast[len(flipLast)-1] ^= 0x01

	tests := map[string]struct {
		data     []byte
		validLen int
		lastSeq  uint64
	}{
		"full-length last record with zeroed bytes": {zeroSector, lastStart, 2},
		"zero-filled tail after last record":        {zeroTail, len(full), 3},
		"bit flip in last record":                   {flipLast, lastStart, 2},
	}
	for name, tt := range tests {
		_, info, err := scan(tt.data, 1, true)
		if err != nil || info.validLen != tt.validLen || info.lastSeq != tt.lastSeq || info.validLen+info.tornLen != len(tt.data) {
			t.Errorf("%s: info %+v, err %v; want torn at %d after seq %d", name, info, err, tt.validLen, tt.lastSeq)
		}
	}
}

// Damage with acknowledged records after it must be ErrCorrupt in the last
// segment too. That holds even when the damage looks exactly like a torn
// tail on its own (edge cases 1.3 and 1.4).
func TestScanMidLogDamageIsCorrupt(t *testing.T) {
	full, offsets := buildSegment(t, 1, 5)
	mid := offsets[2] // record seq 3, with seqs 4 and 5 after it

	flipPayload := bytes.Clone(full)
	flipPayload[mid+headerSize+2] ^= 0x40
	// The length field now claims the record runs past EOF: on its own, it is
	// indistinguishable from a record whose write was cut off. Only the forward
	// scan, finding seq 4 intact, reveals the truth.
	lengthPastEOF := bytes.Clone(full)
	binary.LittleEndian.PutUint32(lengthPastEOF[mid:], uint32(len(full)))
	lengthHuge := bytes.Clone(full)
	binary.LittleEndian.PutUint32(lengthHuge[mid:], MaxPayload+1)
	zeroedMiddle := bytes.Clone(full)
	clear(zeroedMiddle[mid : mid+headerSize])

	for name, data := range map[string][]byte{
		"bit flip in payload":        flipPayload,
		"length points past EOF":     lengthPastEOF,
		"length beyond MaxPayload":   lengthHuge,
		"record header zeroed":       zeroedMiddle,
		"garbage before last record": append(slices.Clone(full[:offsets[4]]), append([]byte("junk"), full[offsets[4]:]...)...),
	} {
		// Records before the damage may already have been applied. That's
		// fine: the caller aborts recovery and discards the state.
		if _, _, err := scan(data, 1, true); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
}

func TestScanPropagatesApplyError(t *testing.T) {
	data, _ := buildSegment(t, 1, 3)
	errStop := errors.New("apply failed")
	_, err := scanSegment("seg", data, 1, true, func(r Record) error {
		if r.Seq == 2 {
			return errStop
		}
		return nil
	})
	if !errors.Is(err, errStop) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want the apply error and not ErrCorrupt", err)
	}
}

// The classifier must agree with real crashes: whatever a crash during Append
// leaves behind is a torn tail at worst, never ErrCorrupt, and keeps every
// acknowledged record. This ties scanSegment to Writer through crashfs.
func TestScanAfterRealCrashesIsNeverCorrupt(t *testing.T) {
	for seed := range uint64(200) {
		rng := rand.New(rand.NewPCG(seed, 0))
		fsys := newCrashFS()
		w := mustCreate(t, fsys, 1)
		crashAtOp(fsys, 1+rng.IntN(80), crashfs.Random(rng))
		var acked uint64
		for i := 0; ; i++ {
			seq, err := w.Append(OpPut, fmt.Appendf(nil, "k%d", i), bytes.Repeat([]byte{byte(i)}, rng.IntN(2000)))
			if err != nil {
				break
			}
			acked = seq
		}
		fsys.SetHook(nil)

		data := []byte(vfstest.ReadFile(t, fsys, filepath.Join(testDir, segmentName(1))))
		_, info, err := scan(data, 1, true)
		if err != nil {
			t.Fatalf("seed %d: real crash classified as %v", seed, err)
		}
		if info.lastSeq < acked {
			t.Fatalf("seed %d: last good seq %d, but %d was acknowledged", seed, info.lastSeq, acked)
		}
		if info.lastSeq > acked+1 {
			t.Fatalf("seed %d: last good seq %d, beyond the in-flight record %d", seed, info.lastSeq, acked+1)
		}
	}
}

// FuzzScanSegment: arbitrary segment bytes never panic. On success, the
// valid and torn parts exactly cover the file, and the valid part re-encodes
// from the applied records byte for byte.
func FuzzScanSegment(f *testing.F) {
	seed, _ := buildSegment(f, 1, 3)
	f.Add(seed, true)
	f.Add(seed[:len(seed)-3], true)
	f.Fuzz(func(t *testing.T, data []byte, last bool) {
		rebuilt := encodeSegmentHeader(1)
		info, err := scanSegment("fuzz", data, 1, last, func(r Record) error {
			var encErr error
			rebuilt, encErr = AppendRecord(rebuilt, r)
			return encErr
		})
		if err != nil {
			return
		}
		if info.validLen+info.tornLen != len(data) || (!last && info.tornLen != 0) {
			t.Fatalf("info %+v doesn't cover %d bytes (last=%v)", info, len(data), last)
		}
		if !bytes.Equal(rebuilt, data[:info.validLen]) {
			t.Fatal("valid prefix doesn't re-encode from the applied records")
		}
	})
}
