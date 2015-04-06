package base


type Checkpointer interface {
	GetCheckpoint(key string) string
	WriteCheckpoint(key string) error
}


type LocalFileCheckpointer struct {
	filePath string
}


func (*LocalFileCheckpointer) GetCheckpoint(key string) string {
	return ""
}


func (*LocalFileCheckpointer) WriteCheckpoint(key string) error {
	return nil
}
