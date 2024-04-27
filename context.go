package dcp

type Context struct {
	Message any
	Ack     func() error
}
