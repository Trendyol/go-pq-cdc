package dcpg

type Context struct {
	Message any
	Ack     func() error
}
