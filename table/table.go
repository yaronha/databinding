package table

import (
	"github.com/yaronha/databinding/datactx"
	"github.com/yaronha/databinding/datasources"
)

func NewTableContext(dc *datactx.DataContextCfg, datasource datasources.DataSource) *TableContext {
	return &TableContext{dc:dc, datasource: datasource}

}

type TableContext struct {
	dc             *datactx.DataContextCfg
	datasource     datasources.DataSource
}

func (tc *TableContext) Read(path string, keys ...interface{}) *TableReader {
	return NewReader(tc.dc, tc.datasource, path, keys)
}

func (tc *TableContext) Write(path string) *TableWriter {
	return NewWriter(tc.dc, tc.datasource, path)
}


