// Package slice provides utility functions for slice type conversions.
package slice

// ConvertToInt converts a byte slice to an int slice.
func ConvertToInt(ss []byte) []int {
	r := make([]int, len(ss))
	for i, s := range ss {
		r[i] = int(s)
	}

	return r
}
