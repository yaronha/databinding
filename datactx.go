package databinding

import (
	"github.com/yaronha/databinding/datactx"
	"github.com/yaronha/databinding/table"
	"github.com/yaronha/databinding/datasources"

	_ "github.com/yaronha/databinding/datasources/v3io"
	"github.com/nuclio/logger"
)


func NewDataContext(logger logger.Logger, config map[string]datasources.DataSourceCfg) *DataContext {
	dc := DataContext{}
	dc.cfg = &datactx.DataContextCfg{Logger: logger.GetChild("datactx")}
	dc.cfg.Sources = createDataSources(dc.cfg.Logger, config)
	logger.Debug(dc.cfg.Sources)

	dc.Table = table.NewTableContext(dc.cfg)
	return &dc
}


type DataContext struct {
	cfg    *datactx.DataContextCfg
	Table  *table.TableContext
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