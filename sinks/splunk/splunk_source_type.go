package splunk

import (
	"github.com/chenziliang/descartes/base"
)

func SourceAndSourcetype(config base.BaseConfig) (string, string) {
	source, sourcetype := config[base.Source], config[base.Sourcetype]
	if source != "" || sourcetype != "" {
		return source, sourcetype
	}

	switch config[base.App] {
	case "snow":
		source, sourcetype = "snow:"+config[base.Metric], "snow"
	case base.KafkaApp:
		source = config[base.Metric]
		sourcetype = base.KafkaApp
	}
	return source, sourcetype
}
