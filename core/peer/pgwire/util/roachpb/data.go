package roachpb

import (
	"bytes"
	"fmt"
	"strconv"
)

var (
	// RKeyMin is a minimum key value which sorts before all other keys.
	RKeyMin = RKey("")
	// KeyMin is a minimum key value which sorts before all other keys.
	KeyMin = Key(RKeyMin)
	// RKeyMax is a maximum key value which sorts after all other keys.
	RKeyMax = RKey{0xff, 0xff}
	// KeyMax is a maximum key value which sorts after all other keys.
	KeyMax = Key(RKeyMax)

	// PrettyPrintKey is a function to print key with human readable format
	// it's implement at package git.com/cockroachdb/cockroach/keys to avoid package circle import
	PrettyPrintKey func(key Key) string
)

// RKey denotes a Key whose local addressing has been accounted for.
// A key can be transformed to an RKey by keys.Addr().
type RKey Key

// AsRawKey returns the RKey as a Key. This is to be used only in select
// situations in which an RKey is known to not contain a wrapped locally-
// addressed Key. Whenever the Key which created the RKey is still available,
// it should be used instead.
func (rk RKey) AsRawKey() Key {
	return Key(rk)
}

// Less compares two RKeys.
func (rk RKey) Less(otherRK RKey) bool {
	return bytes.Compare(rk, otherRK) < 0
}

// Equal checks for byte-wise equality.
func (rk RKey) Equal(other []byte) bool {
	return bytes.Equal(rk, other)
}

// Next returns the RKey that sorts immediately after the given one.
// The method may only take a shallow copy of the RKey, so both the
// receiver and the return value should be treated as immutable after.
func (rk RKey) Next() RKey {
	return RKey(BytesNext(rk))
}

// PrefixEnd determines the end key given key as a prefix, that is the
// key that sorts precisely behind all keys starting with prefix: "1"
// is added to the final byte and the carry propagated. The special
// cases of nil and KeyMin always returns KeyMax.
func (rk RKey) PrefixEnd() RKey {
	if len(rk) == 0 {
		return RKeyMax
	}
	return RKey(bytesPrefixEnd(rk))
}

func (rk RKey) String() string {
	return Key(rk).String()
}

// Key is a custom type for a byte string in proto
// messages which refer to Cockroach keys.
type Key []byte

// BytesNext returns the next possible byte slice, using the extra capacity
// of the provided slice if possible, and if not, appending an \x00.
func BytesNext(b []byte) []byte {
	if cap(b) > len(b) {
		bNext := b[:len(b)+1]
		if bNext[len(bNext)-1] == 0 {
			return bNext
		}
	}
	// TODO(spencer): Do we need to enforce KeyMaxLength here?
	// Switched to "make and copy" pattern in #4963 for performance.
	bn := make([]byte, len(b)+1)
	copy(bn, b)
	bn[len(bn)-1] = 0
	return bn
}

func bytesPrefixEnd(b []byte) []byte {
	// Switched to "make and copy" pattern in #4963 for performance.
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end
		}
	}
	// This statement will only be reached if the key is already a
	// maximal byte string (i.e. already \xff...).
	return b
}

// Next returns the next key in lexicographic sort order. The method may only
// take a shallow copy of the Key, so both the receiver and the return
// value should be treated as immutable after.
func (k Key) Next() Key {
	return Key(BytesNext(k))
}

// IsPrev is a more efficient version of k.Next().Equal(m).
func (k Key) IsPrev(m Key) bool {
	l := len(m) - 1
	return l == len(k) && m[l] == 0 && k.Equal(m[:l])
}

// PrefixEnd determines the end key given key as a prefix, that is the
// key that sorts precisely behind all keys starting with prefix: "1"
// is added to the final byte and the carry propagated. The special
// cases of nil and KeyMin always returns KeyMax.
func (k Key) PrefixEnd() Key {
	if len(k) == 0 {
		return Key(RKeyMax)
	}
	return Key(bytesPrefixEnd(k))
}

// Equal returns whether two keys are identical.
func (k Key) Equal(l Key) bool {
	return bytes.Equal(k, l)
}

// Compare implements the interval.Comparable interface for tree nodes.
func (k Key) Compare(l Key) int {
	return bytes.Compare(k, l)
}

// String returns a string-formatted version of the key.
func (k Key) String() string {
	if PrettyPrintKey != nil {
		return PrettyPrintKey(k)
	}

	return fmt.Sprintf("%q", []byte(k))
}

// Format implements the fmt.Formatter interface.
func (k Key) Format(f fmt.State, verb rune) {
	// Note: this implementation doesn't handle the width and precision
	// specifiers such as "%20.10s".
	if PrettyPrintKey != nil {
		fmt.Fprint(f, PrettyPrintKey(k))
	} else {
		fmt.Fprintf(f, strconv.Quote(string(k)))
	}
}
