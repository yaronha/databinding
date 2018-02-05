package datactx

import (
	"github.com/yaronha/databinding/datasources"
	"strings"
	"fmt"
	"github.com/nuclio/logger"
)

type DataContextCfg struct {
	Logger       logger.Logger
	Sources      map[string]datasources.DataSource
	SecretStore  SecretProvider
	WaitGroups   map[int][]DoResult
}

type DoResult interface {
	Result() (interface{}, error)
}


type SecretProvider interface {
	GetSecrets(name, requieredBy string) map[string][]byte
}

func (dc *DataContextCfg) GetFullpath(ds datasources.DataSource, path string) string {
	return ds.GetConfig().BasePath + path
}

// Take the path string and returns the data binding & path (with basePath)
func (dc *DataContextCfg) GetSource(path string) (datasources.DataSource, string, error) {
	if len(dc.Sources) == 0 {
		return nil, "", fmt.Errorf("There are zero data bindings, cannot return")
	}

	i := strings.Index(path, "://")
	// if no binding prefix return the first data binding
	if i < 0 {
		for _, ds := range dc.Sources {
			path = ds.GetConfig().BasePath + path
			return ds, path, nil
		}
	}

	name := path[0:i]
	ds, ok := dc.Sources[name]
	if !ok {
		return nil, "", fmt.Errorf("data binding named %s not found", name)
	}

	if len(path) <= i+3 {
		return ds, "", nil
	}

	return ds, path[i+3: len(path)], nil
}