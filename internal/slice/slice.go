package slice

func ConvertToInt(ss []byte) []int {
	r := make([]int, len(ss))
	for i, s := range ss {
		r[i] = int(s)
	}

	return r
}
