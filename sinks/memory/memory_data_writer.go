package memory

import (
	"github.com/chenziliang/descartes/base"
)

type MemoryDataWriter struct {
	dataChan chan *base.Data
}

func NewMemoryDataWriter() *MemoryDataWriter {
	return &MemoryDataWriter{
		dataChan: make(chan *base.Data, 16),
	}
}

func (writer *MemoryDataWriter) Start() {
}

func (writer *MemoryDataWriter) Stop() {
}

func (writer *MemoryDataWriter) WriteData(data *base.Data) error {
	return writer.doWriteData(data)
}

func (writer *MemoryDataWriter) WriteDataSync(data *base.Data) error {
	return writer.doWriteData(data)
}

func (writer *MemoryDataWriter) WriteDataAsync(data *base.Data) error {
	return writer.doWriteData(data)
}

func (writer *MemoryDataWriter) doWriteData(data *base.Data) error {
	writer.dataChan <- data
	return nil
}

func (writer *MemoryDataWriter) Data() <-chan *base.Data {
	return writer.dataChan
}
