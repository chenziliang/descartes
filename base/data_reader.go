package base

type DataReader interface {
	Start()
	Stop()
	ReadData() error
	IndexData() error
}
