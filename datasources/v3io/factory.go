package v3io

import (
	"github.com/yaronha/databinding/datasources"
	"github.com/nuclio/logger"
)

type factory struct{}

func (f *factory) Create(parentLogger logger.Logger, name string,
	dsConfiguration *datasources.DataSourceCfg) (datasources.DataSource, error) {

	return NewV3ioDataSource(parentLogger.GetChild("v3io-ds"), dsConfiguration)
}

// register factory
func init() {
	datasources.RegistrySingleton.Register("v3io", &factory{})
}

