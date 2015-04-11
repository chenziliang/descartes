package base


type BaseConfig struct {
	ServerURL string
	Username string
	Password string
	ProxyURL string
	ProxyUsername string
	ProxyPassword string
	AdditionalConfig map[string] string
}

type Event struct {
	*BaseConfig
	rawEvents []string
}


func NewEvent(config *BaseConfig) *Event {
	return &Event {
		BaseConfig: config,
		rawEvents: make([]string, 10),
	}
}

func (e *Event) Add(rawEvent string) {
	e.rawEvents = append(e.rawEvents, rawEvent)
}

func (e *Event) RawEvents() []string {
	return e.rawEvents
}
