package dcpg

import (
	"github.com/3n0ugh/dcpg/pq"
)

type Stream interface {
	Open()
}

type stream struct {
	conn pq.Connection
}

func (s *stream) listen() {

}
