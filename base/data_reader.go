package base

type DataReader interface {
	Start()
	Stop()
	ReadData() ([]byte, error)
	IndexData() error
}
