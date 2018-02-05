package databinding

import (
	"github.com/yaronha/databinding/datactx"
	"github.com/yaronha/databinding/table"
	"github.com/yaronha/databinding/datasources"

	_ "github.com/yaronha/databinding/datasources/v3io"
	"github.com/nuclio/logger"
	"fmt"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/nuclio/zap"
)

// Create a new data context from configuration
func NewDataContext(config map[string]datasources.DataSourceCfg, verbose bool) (*DataContext, error) {

	var logLevel nucliozap.Level
	if verbose {
		logLevel = nucliozap.DebugLevel
	} else {
		logLevel = nucliozap.WarnLevel
	}
	logger, err := nucliozap.NewNuclioZapCmd("v3test", logLevel)
	if err != nil {
		return nil, err
	}


	dc := DataContext{}
	dc.cfg = &datactx.DataContextCfg{Logger: logger.GetChild("datactx")}
	dc.cfg.Sources = createDataSources(dc.cfg.Logger, config)
	dc.cfg.WaitGroups = map[int][]datactx.DoResult{}
	logger.Debug(dc.cfg.Sources)

	//dc.Table = table.NewTableContext(dc.cfg)
	return &dc, nil
}

// create a new data context from nuclio data bindings
func NewFromContext(context *nuclio.Context) *DataContext {
	dc := DataContext{}
	dc.cfg = &datactx.DataContextCfg{Logger: context.Logger.GetChild("datactx")}
	dsMap := map[string]datasources.DataSource{}

	for name, datasource := range context.DataBinding {
		dscfg := datasources.DataSourceCfg{}
		dscfg.FromContext = datasource
		ds, _ := datasources.RegistrySingleton.NewDataSource(dc.cfg.Logger,"v3io",name,
			&dscfg)
		dsMap[name] = ds
	}

	dc.cfg.Sources = dsMap
	dc.cfg.WaitGroups = map[int][]datactx.DoResult{}
	context.Logger.Debug(dc.cfg.Sources)

	//dc.Table = table.NewTableContext(dc.cfg)
	return &dc
}


type DataContext struct {
	cfg    *datactx.DataContextCfg
	//Table  *table.TableContext
}

type AsyncResponse struct {
	Result  interface{}
	Err     error
}

func (dc *DataContext) GetLogger() logger.Logger {
	return dc.cfg.Logger
}

func (dc *DataContext) Table(databinding string) *table.TableContext {
	ds, ok := dc.cfg.Sources[databinding]
	if !ok {
		return nil  //, fmt.Errorf("data binding named %s not found", databinding)
	}

	return table.NewTableContext(dc.cfg, ds)
}

func (dc *DataContext) Raw(databinding string) (interface{}, error) {
	ds, ok := dc.cfg.Sources[databinding]
	if !ok {
		return nil, fmt.Errorf("data binding named %s not found", databinding)
	}

	return ds.GetRaw()
}

func (dc *DataContext) Wait(waitGroup int) ([]AsyncResponse, error) {
	responses := []AsyncResponse{}
	var totalErr error

	_, ok := dc.cfg.WaitGroups[waitGroup]
	if !ok {
		return responses, fmt.Errorf("No such wait group (%d)", waitGroup)
	}

	for _, request := range dc.cfg.WaitGroups[waitGroup] {
		result, err := request.Result()
		responses = append(responses, AsyncResponse{Result:result, Err:err})
		if err != nil {
			totalErr = fmt.Errorf("error(s) in the responses")
		}
	}

	// release the wait group
	delete(dc.cfg.WaitGroups, waitGroup)

	return responses, totalErr
}


func createDataSources(logger logger.Logger, config map[string]datasources.DataSourceCfg) map[string]datasources.DataSource {
	dsMap := map[string]datasources.DataSource{}
	for name, datasource := range config {
		ds, _ := datasources.RegistrySingleton.NewDataSource(logger,"v3io",name,
			&datasource)
		dsMap[name] = ds
	}

	return dsMap
}