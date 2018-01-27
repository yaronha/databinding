package datasources

import (
	"github.com/nuclio/nuclio/pkg/registry"
	"github.com/yaronha/databinding/requests"
	"github.com/nuclio/logger"
)


type DataSource interface {
	GetConfig()  *DataSourceCfg
	TableReadReq(req *requests.ReadRequest) (requests.ReadResponse, error)
	TableWriteReq(req *requests.WriteRequest) (requests.ExecResponse, error)

}

type DataSourceCfg struct {
	Class     string            `json:"class"`
	URL       string            `json:"url"`
	Resource  string            `json:"resource,omitempty"`
	BasePath  string            `json:"path,omitempty"`
	Query     string            `json:"query,omitempty"`
	Secret    string            `json:"secret,omitempty"`
	Options   map[string]string `json:"options,omitempty"`
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

