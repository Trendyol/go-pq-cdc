package integration

import (
	"strconv"
	"sync/atomic"
)

type Book struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func (b *Book) Map() map[string]any {
	return map[string]any{
		"id":   int32(b.ID),
		"name": b.Name,
	}
}

func CreateBooks(count int) []Book {
	var idCounter atomic.Int64
	res := make([]Book, count)
	for i := 0; i < count; i++ {
		id := int(idCounter.Add(1))
		res[i] = Book{
			ID:   id,
			Name: "book-no-" + strconv.Itoa(id),
		}
	}
	return res
}
