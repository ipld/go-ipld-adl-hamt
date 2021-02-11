package hamt

import (
	"math/big"
	"math/bits"
)

// TODO: These math/big.Int funcs are slow; we can add some caching later.

func bitsetGetv3(p []byte, i int) bool {
	z := new(big.Int).SetBytes(p)
	return z.Bit(i) != 0
}

func bitsetSetv3(p []byte, i int) []byte {
	z := new(big.Int).SetBytes(p)
	z.SetBit(z, i, 1)
	return z.Bytes()
}

// indexForBitPos is copied as-is from filecoin/go-hamt-ipld.
func indexForBitPos(bp int, bitfield *big.Int) int {
	var x uint
	var count, i int
	w := bitfield.Bits()
	for x = uint(bp); x > bits.UintSize && i < len(w); x -= bits.UintSize {
		count += bits.OnesCount(uint(w[i]))
		i++
	}
	if i == len(w) {
		return count
	}
	return count + bits.OnesCount(uint(w[i])&((1<<x)-1))
}

func onesCountRangev3(p []byte, to int) int {
	z := new(big.Int).SetBytes(p)
	return indexForBitPos(to, z)
}
