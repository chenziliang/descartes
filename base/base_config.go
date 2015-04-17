package base

type BaseConfig struct {
	ServerURL        string
	Username         string
	Password         string
	ProxyURL         string
	ProxyUsername    string
	ProxyPassword    string
	AdditionalConfig map[string]string
}

type Data struct {
	MetaInfo map[string]string
	RawData  [][]byte
}

func NewData(metaInfo map[string]string, rawData [][]byte) *Data {
	return &Data{
		MetaInfo: metaInfo,
		RawData:  rawData,
	}
}
