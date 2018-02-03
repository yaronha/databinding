package v3io

import (
	"github.com/v3io/v3io-go-http"
	"github.com/yaronha/databinding/datasources"
	"github.com/pkg/errors"
	"github.com/nuclio/logger"
)

const MAX_REQ_CHANNEL  = 100

func NewV3ioDataSource(logger logger.Logger, cfg *datasources.DataSourceCfg) (datasources.DataSource, error) {
	newV3IOds := v3ioDS{logger:logger, AbstractDataSource: datasources.AbstractDataSource{Config:cfg}}
	var err error
	newV3IOds.container, err = createContainer(logger, cfg.URL, cfg.Resource)
	if err !=nil {
		return &newV3IOds, err
	}
	return &newV3IOds, nil

}

type v3ioDS struct {
	datasources.AbstractDataSource
	logger      logger.Logger
	container   *v3io.Container
}

func (v *v3ioDS) GetRaw() (interface{}, error) {
	return v.container, nil
}


func createContainer(logger logger.Logger, addr, cont string) (*v3io.Container, error) {
	// create context
	context, err := v3io.NewContext(logger, addr , 8)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create client")
	}

	// create session
	session, err := context.NewSession("", "", "v3test")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create session")
	}

	// create the container
	container, err := session.NewContainer(cont)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create container")
	}

	return container, nil
}

func (v *v3ioDS) GetConfig() *datasources.DataSourceCfg {
	return v.Config
}
