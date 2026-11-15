package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func mustEncode(t testing.TB, r Record) []byte {
	t.Helper()
	b, err := AppendRecord(nil, r)
	if err != nil {
		t.Fatalf("AppendRecord(%+v): %v", r, err)
	}
	return b
}

func sameRecord(a, b Record) bool {
	return a.Seq == b.Seq && a.Op == b.Op && bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value)
}

func TestRecordRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		r    Record
	}{
		{"put", Record{Seq: 1, Op: OpPut, Key: []byte("k"), Value: []byte("v")}},
		{"put empty value", Record{Seq: 2, Op: OpPut, Key: []byte("k")}},
		{"delete", Record{Seq: 3, Op: OpDelete, Key: []byte("k")}},
		{"max sizes", Record{Seq: 4, Op: OpPut, Key: bytes.Repeat([]byte{0xff}, MaxKeySize), Value: bytes.Repeat([]byte{0}, MaxValueSize)}},
		{"max seq", Record{Seq: ^uint64(0), Op: OpPut, Key: []byte{0}, Value: []byte{0}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := mustEncode(t, tt.r)
			got, n, err := DecodeRecord(buf)
			if err != nil || n != len(buf) || !sameRecord(got, tt.r) {
				t.Fatalf("DecodeRecord = %+v, %d, %v; want %+v, %d, nil", got, n, err, tt.r, len(buf))
			}
		})
	}
}

// Records appended to one buffer decode back in order, and the existing
// contents of dst are left alone.
func TestAppendRecordAppends(t *testing.T) {
	r1 := Record{Seq: 1, Op: OpPut, Key: []byte("a"), Value: []byte("1")}
	r2 := Record{Seq: 2, Op: OpDelete, Key: []byte("a")}
	buf := []byte("prefix")
	for _, r := range []Record{r1, r2} {
		var err error
		if buf, err = AppendRecord(buf, r); err != nil {
			t.Fatalf("AppendRecord(%+v): %v", r, err)
		}
	}
	if !bytes.HasPrefix(buf, []byte("prefix")) {
		t.Fatal("AppendRecord overwrote the existing contents of dst")
	}
	rest := buf[len("prefix"):]
	for _, want := range []Record{r1, r2} {
		got, n, err := DecodeRecord(rest)
		if err != nil || !sameRecord(got, want) {
			t.Fatalf("DecodeRecord = %+v, %v; want %+v", got, err, want)
		}
		rest = rest[n:]
	}
	if len(rest) != 0 {
		t.Fatalf("%d bytes left over", len(rest))
	}
}

func TestAppendRecordRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		r    Record
	}{
		{"zero op", Record{Key: []byte("k")}},
		{"unknown op", Record{Op: 3, Key: []byte("k")}},
		{"empty key", Record{Op: OpPut}},
		{"key too large", Record{Op: OpPut, Key: make([]byte, MaxKeySize+1)}},
		{"value too large", Record{Op: OpPut, Key: []byte("k"), Value: make([]byte, MaxValueSize+1)}},
		{"delete with value", Record{Op: OpDelete, Key: []byte("k"), Value: []byte("v")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := []byte("keep")
			got, err := AppendRecord(dst, tt.r)
			if !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("err = %v, want ErrInvalidRecord", err)
			}
			if string(got) != "keep" {
				t.Fatalf("dst = %q after error, want unchanged", got)
			}
		})
	}
}

// TestDecodeDetectsEveryBitFlip flips each bit of encoded records, one at a
// time. A flip in the crc, seq or payload is guaranteed to be caught: a CRC
// detects every error burst no longer than itself (32 bits). A flip in the
// length field either makes the record look truncated or changes which bytes
// are checksummed. That detection is probabilistic in general (miss chance
// 2^-32), but these inputs are fixed, so the test is deterministic.
func TestDecodeDetectsEveryBitFlip(t *testing.T) {
	for _, r := range []Record{
		{Seq: 7, Op: OpPut, Key: []byte("user:42"), Value: []byte("hello, world")},
		{Seq: 8, Op: OpDelete, Key: []byte("user:42")},
	} {
		orig := mustEncode(t, r)
		for bit := range len(orig) * 8 {
			buf := bytes.Clone(orig)
			buf[bit/8] ^= 1 << (bit % 8)
			if _, _, err := DecodeRecord(buf); err == nil {
				t.Fatalf("record %+v: flipping bit %d went undetected", r, bit)
			}
		}
	}
}

func TestDecodeTruncatedAtEveryLength(t *testing.T) {
	buf := mustEncode(t, Record{Seq: 1, Op: OpPut, Key: []byte("key"), Value: []byte("value")})
	for n := range len(buf) {
		if _, _, err := DecodeRecord(buf[:n]); !errors.Is(err, ErrTruncated) {
			t.Fatalf("first %d of %d bytes: err = %v, want ErrTruncated", n, len(buf), err)
		}
	}
}

func TestDecodeOversizedLengthIsBadNotTruncated(t *testing.T) {
	buf := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(buf, MaxPayload+1)
	if _, _, err := DecodeRecord(buf); !errors.Is(err, ErrBadRecord) {
		t.Fatalf("err = %v, want ErrBadRecord", err)
	}
}

// Pins the documented behavior: decoded slices point into the input, a
// successful decode allocates nothing, and appending to a decoded key can't
// overwrite the bytes after it.
func TestDecodeAliasesInput(t *testing.T) {
	buf := mustEncode(t, Record{Seq: 1, Op: OpPut, Key: []byte("k"), Value: []byte("v")})
	r, _, err := DecodeRecord(buf)
	if err != nil {
		t.Fatal(err)
	}
	if cap(r.Key) != len(r.Key) {
		t.Fatalf("cap(Key) = %d, want %d", cap(r.Key), len(r.Key))
	}
	r.Key[0] = 'X'
	if !bytes.Contains(buf, []byte("X")) {
		t.Fatal("Key is a copy; expected it to point into buf")
	}
	clean := mustEncode(t, Record{Seq: 1, Op: OpPut, Key: []byte("k"), Value: []byte("v")})
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, err := DecodeRecord(clean); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("DecodeRecord allocated %v times, want 0", allocs)
	}
}

// frame wraps an arbitrary payload in a header with a correct checksum, to
// build records that pass the CRC check but carry a malformed payload, the
// kind a buggy writer would produce.
func frame(payload []byte) []byte {
	rec := make([]byte, headerSize, headerSize+len(payload))
	rec = append(rec, payload...)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint64(rec[8:16], 1)
	binary.LittleEndian.PutUint32(rec[4:8], checksum(rec))
	return rec
}

func TestDecodeMalformedPayloadWithValidChecksum(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{"empty payload", nil},
		{"zero op", []byte{0, 1, 'k'}},
		{"unknown op", []byte{9, 1, 'k'}},
		{"missing key length", []byte{byte(OpDelete)}},
		{"key length past end", []byte{byte(OpDelete), 5, 'k'}},
		{"non-minimal varint", []byte{byte(OpDelete), 0x81, 0x00, 'k'}},
		{"empty key", []byte{byte(OpDelete), 0}},
		{"put missing value", []byte{byte(OpPut), 1, 'k'}},
		{"delete trailing bytes", []byte{byte(OpDelete), 1, 'k', 'x'}},
		{"put trailing bytes", []byte{byte(OpPut), 1, 'k', 1, 'v', 'x'}},
		{"key too large", append([]byte{byte(OpDelete), 0x81, 0x20}, make([]byte, MaxKeySize+1)...)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := DecodeRecord(frame(tt.payload)); !errors.Is(err, ErrBadRecord) {
				t.Fatalf("err = %v, want ErrBadRecord", err)
			}
		})
	}
}

// FuzzDecodeRecord feeds arbitrary bytes to the decoder. It must never panic,
// must fail only with ErrTruncated or ErrBadRecord, and anything it accepts
// must re-encode to exactly the bytes it consumed (one encoding per record).
func FuzzDecodeRecord(f *testing.F) {
	f.Add(mustEncode(f, Record{Seq: 1, Op: OpPut, Key: []byte("k"), Value: []byte("v")}))
	f.Add(mustEncode(f, Record{Seq: 2, Op: OpDelete, Key: []byte("key")}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		r, n, err := DecodeRecord(data)
		if err != nil {
			if !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrBadRecord) {
				t.Fatalf("unexpected error kind: %v", err)
			}
			return
		}
		if n < headerSize || n > len(data) {
			t.Fatalf("consumed %d of %d bytes", n, len(data))
		}
		if again := mustEncode(t, r); !bytes.Equal(again, data[:n]) {
			t.Fatalf("re-encoding differs:\n got %x\nwant %x", again, data[:n])
		}
	})
}

// FuzzRecordRoundTrip: every record AppendRecord accepts decodes back to
// itself, and everything it rejects is ErrInvalidRecord.
func FuzzRecordRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint8(OpPut), []byte("k"), []byte("v"))
	f.Add(uint64(2), uint8(OpDelete), []byte("k"), []byte(nil))
	f.Fuzz(func(t *testing.T, seq uint64, op uint8, key, value []byte) {
		in := Record{Seq: seq, Op: Op(op), Key: key, Value: value}
		buf, err := AppendRecord(nil, in)
		if err != nil {
			if !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("unexpected error kind: %v", err)
			}
			return
		}
		out, n, err := DecodeRecord(buf)
		if err != nil || n != len(buf) || !sameRecord(in, out) {
			t.Fatalf("round trip: got %+v, %d, %v; want %+v", out, n, err, in)
		}
	})
}
