package hamt

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestRangedInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bs       []byte
		from, to int
		want     int
	}{
		{[]byte{0b00001111}, 0, 8, 0x0f},
		{[]byte{0b00001111}, 0, 4, 0x00},
		{[]byte{0b00001111}, 4, 8, 0x0f},
		{[]byte{0b00001111}, 4, 6, 0b0011},
		{[]byte{0b11111111}, 4, 8, 0x0f},
		{[]byte{0b00001111}, 3, 5, 0b0001},

		{[]byte{0b11111111, 0b00000000}, 8, 16, 0x00},
		{[]byte{0b11111111, 0b00000000}, 0, 16, 0xff00},
		{[]byte{0b11111111, 0b00000000}, 4, 12, 0xf0},
		{[]byte{0b11111111, 0b00000000}, 6, 10, 0b1100},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%x-%d-%d", test.bs, test.from, test.to), func(t *testing.T) {
			got := rangedInt(test.bs, test.from, test.to)
			qt.Assert(t, got, qt.Equals, test.want)
		})
	}
}

func TestBitset(t *testing.T) {
	t.Parallel()

	bs := []byte{0b00001111, 0b01010101}

	qt.Assert(t, bitsetGet(bs, 0), qt.IsFalse)
	qt.Assert(t, bitsetGet(bs, 3), qt.IsFalse)
	qt.Assert(t, bitsetGet(bs, 4), qt.IsTrue)
	qt.Assert(t, bitsetGet(bs, 7), qt.IsTrue)

	qt.Assert(t, bitsetGet(bs, 8), qt.IsFalse)
	qt.Assert(t, bitsetGet(bs, 10), qt.IsFalse)
	qt.Assert(t, bitsetGet(bs, 13), qt.IsTrue)
	qt.Assert(t, bitsetGet(bs, 15), qt.IsTrue)

	bitsetClear(bs, 15)
	qt.Assert(t, bitsetGet(bs, 15), qt.IsFalse)
	bitsetSet(bs, 15)
	qt.Assert(t, bitsetGet(bs, 15), qt.IsTrue)

	qt.Assert(t, onesCountRange(bs, 4), qt.Equals, 0)
	qt.Assert(t, onesCountRange(bs, 8), qt.Equals, 4)
	qt.Assert(t, onesCountRange(bs, 9), qt.Equals, 4)
	qt.Assert(t, onesCountRange(bs, 10), qt.Equals, 5)
}
