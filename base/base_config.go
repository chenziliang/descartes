package base


type BaseConfig struct {
	ServerURL string
	Username string
	Password string
	ProxyURL string
	ProxyUsername string
	ProxyPassword string
	AdditionalConfig map[string]string
}

type Event struct {
	MetaInfo map[string]string
	RawEvents [][]byte
}


func NewEvent(metaInfo map[string]string, rawEvents [][]byte) *Event {
	return &Event {
		MetaInfo: metaInfo,
		RawEvents: rawEvents,
	}
}
