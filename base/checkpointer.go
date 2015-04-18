package base

type Checkpointer interface {
	GetCheckpoint(keyInfo map[string]string) ([]byte, error)
	WriteCheckpoint(keyInfo map[string]string, value []byte) error
	DeleteCheckpoint(keyInfo map[string]string) error
}
