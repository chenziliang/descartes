package base

import (
	"fmt"
)

type DataWriter interface {
	Start()
	Stop()
	WriteData(data *Data) error // can be sync or async
	WriteDataSync(data *Data) error
	WriteDataAsync(data *Data) error
}

type StdoutDataWriter struct {
}

func (d *StdoutDataWriter) Start() {
}

func (d *StdoutDataWriter) Stop() {
}

func (d *StdoutDataWriter) WriteData(data *Data) error {
	return d.doWriteData(data)
}

func (d *StdoutDataWriter) WriteDataSync(data *Data) error {
	return d.doWriteData(data)
}

func (d *StdoutDataWriter) WriteDataAsync(data *Data) error {
	return d.doWriteData(data)
}

func (d *StdoutDataWriter) doWriteData(data *Data) error {
	for i := 0; i < len(data.RawData); i++ {
		fmt.Println(string(data.RawData[i]))
	}
	return nil
}
