package table

import (
	"github.com/yaronha/databinding/datactx"
)

func NewTableContext(dc *datactx.DataContextCfg) * TableContext {
	return &TableContext{dc:dc}

}

type TableContext struct {
	dc             *datactx.DataContextCfg
}

func (tc *TableContext) Read(path string, keys ...interface{}) *TableReader {
	return NewReader(tc.dc, path, keys)
}

func (tc *TableContext) Write(path string) *TableWriter {
	return NewWriter(tc.dc, path)
}


