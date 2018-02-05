package datasources

import (
	"github.com/yaronha/databinding/datasources/registry"
	"github.com/yaronha/databinding/requests"
	"github.com/nuclio/logger"
	"fmt"
)


type DataSource interface {
	GetConfig()  *DataSourceCfg
	GetRaw() (interface{}, error)
	TableReadReq(req *requests.ReadRequest) (requests.ReadResponse, error)
	TableWriteReq(req *requests.WriteRequest) (requests.ExecResponse, error)

}

type AbstractDataSource struct {
	Capabilities  int
	Config        *DataSourceCfg
}

func (ds *AbstractDataSource) GetRaw() (interface{}, error) {
	return nil, fmt.Errorf("Datasource does not support raw interface")
}

func (ds *AbstractDataSource) GetConfig() *DataSourceCfg {
	return ds.Config
}

func (ds *AbstractDataSource) TableReadReq(req *requests.ReadRequest) (requests.ReadResponse, error) {
	return nil, fmt.Errorf("Datasource does not support TableRead interface")
}

func (ds *AbstractDataSource) TableWriteReq(req *requests.WriteRequest) (requests.ExecResponse, error) {
	return nil, fmt.Errorf("Datasource does not support TableWrite interface")
}



type DataSourceCfg struct {
	FromContext  interface{}       `json:"fromContext,omitempty"`
	Class        string            `json:"class"`
	URL          string            `json:"url"`
	Resource     string            `json:"resource,omitempty"`
	BasePath     string            `json:"path,omitempty"`
	Query        string            `json:"query,omitempty"`
	Secret       string            `json:"secret,omitempty"`
	Options      map[string]string `json:"options,omitempty"`
}

type Creator interface {

	// Create creates a trigger instance
	Create(logger.Logger, string, *DataSourceCfg) (DataSource, error)
}

type Registry struct {
	registry.Registry
}

// global singleton
var RegistrySingleton = Registry{
	Registry: *registry.NewRegistry("datasource"),
}

func (r *Registry) NewDataSource(logger logger.Logger,
	kind string, name string, cfg *DataSourceCfg) (DataSource, error) {

	registree, err := r.Get(kind)
	if err != nil {
		return nil, err
	}

	return registree.(Creator).Create(logger, name, cfg)
}

