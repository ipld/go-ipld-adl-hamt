package hamt

import "math/bits"

func rangedInt(p []byte, from, to int) int {
	if from > to || from < 0 || to < 0 {
		panic("invalid range")
	}
	// Skip the leading unused bytes.
	for from >= 8 {
		p = p[1:]
		from -= 8
		to -= 8
	}

	result := 0
	for to > 0 {
		b0 := p[0]
		if from > 0 {
			// Remove remaining leading bits.
			b0 = (b0 << from) >> from
			from = 0
		}
		if to < 8 {
			// Remove the trailing bits we don't want.
			b0 >>= (8 - to)
			result = result << to
		} else {
			result = result << 8
		}
		result |= int(b0)
		p = p[1:]
		to -= 8
	}
	return result
}

func bitsetGet(p []byte, i int) bool {
	return p[i/8]&(1<<(7-i%8)) != 0
}

func bitsetSet(p []byte, i int) {
	n := p[i/8]
	n |= 1 << (7 - i%8)
	p[i/8] = n
}

func bitsetClear(p []byte, i int) {
	n := p[i/8]
	n &^= 1 << (7 - i%8)
	p[i/8] = n
}

func onesCountRange(p []byte, to int) int {
	count := 0
	for to > 0 {
		b := p[0]
		if to < 8 {
			b >>= 8 - to
		}
		count += bits.OnesCount8(b)
		to -= 8
		p = p[1:]
	}
	return count
}
