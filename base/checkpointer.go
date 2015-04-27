package base

type Checkpointer interface {
	Start()
	Stop()
	GetCheckpoint(keyInfo map[string]string) ([]byte, error)
	WriteCheckpoint(keyInfo map[string]string, value []byte) error
	DeleteCheckpoint(keyInfo map[string]string) error
}

type NullCheckpointer struct {
}

func NewNullCheckpoint() *NullCheckpointer {
	return &NullCheckpointer{}
}

func (checkpoint *NullCheckpointer) Start() {
}

func (checkpoint *NullCheckpointer) Stop() {
}

func (checkpoint *NullCheckpointer) GetCheckpoint(keyInfo map[string]string) ([]byte, error) {
	return nil, nil
}

func (checkpoint *NullCheckpointer) WriteCheckpoint(keyInfo map[string]string, value []byte) error {
	return nil
}

func (checkpoint *NullCheckpointer) DeleteCheckpoint(keyInfo map[string]string) error {
	return nil
}
