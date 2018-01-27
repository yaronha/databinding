package v3io

import (
	"github.com/v3io/v3io-go-http"
	"fmt"
	"github.com/yaronha/databinding/datasources"
	"github.com/pkg/errors"
	"github.com/nuclio/logger"
)

const MAX_REQ_CHANNEL  = 100

func NewV3ioDataSource(logger logger.Logger, cfg *datasources.DataSourceCfg) (datasources.DataSource, error) {
	newV3IOds := v3ioDS{logger:logger, cfg:cfg}
	var err error
	newV3IOds.container, err = createContainer(logger, cfg.URL, cfg.Resource)
	if err !=nil {
		return &newV3IOds, err
	}
	newV3IOds.waitGroups = map[int]*waitGroup{}
	return &newV3IOds, nil

}

type v3ioDS struct {
	logger      logger.Logger
	container   *v3io.Container
	cfg         *datasources.DataSourceCfg
	waitGroups  map[int]*waitGroup
}

type waitGroup struct {
	group     map[uint64]interface{}
	respChan  chan *v3io.Response
}

func (v *v3ioDS) addToWaitGroup(group int, reqId uint64, request interface{}) error {
	wg, ok := v.waitGroups[group]
	if !ok {
		wg := waitGroup{}
		wg.group = map[uint64]interface{}{}
		wg.respChan = make(chan *v3io.Response, MAX_REQ_CHANNEL)
		v.waitGroups[group] = &wg
	}

	wg.group[reqId] = request
	return nil
}

func (v *v3ioDS) closeWaitGroup(group int) error {
	wg, ok := v.waitGroups[group]
	if !ok {
		return fmt.Errorf("No such wait group (%d)", group)
	}
	if len(wg.group) > 0 {
		return fmt.Errorf("Group has pending requests, cannot close")
	}
	delete(v.waitGroups, group)
	return nil
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
	return v.cfg
}
