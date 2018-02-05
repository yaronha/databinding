package table

import (
	"github.com/yaronha/databinding/requests"
	"github.com/yaronha/databinding/datactx"
	"fmt"
	"github.com/yaronha/databinding/datasources"
)


func NewReader(dc *datactx.DataContextCfg, datasource datasources.DataSource, path string, keys []interface{}) *TableReader {
	newReader := TableReader{dc: dc, datasource: datasource, path:path}
	newReader.req = &requests.ReadRequest{Keys:keys}
	return &newReader
}

type TableReader struct {
	dc          *datactx.DataContextCfg
	datasource  datasources.DataSource
	req         *requests.ReadRequest
	executed    bool
	path        string
}

func (tr *TableReader) Format(format string) *TableReader {
	tr.req.Format = format
	return tr
}

func (tr *TableReader) Select(attrs ...string) *TableReader {
	tr.req.Attributes = attrs
	return tr
}

func (tr *TableReader) Where(filter string) *TableReader {
	tr.req.Filter = filter
	return tr
}

func (tr *TableReader) Query(sql string, vars ...interface{}) *TableReader {
	tr.req.Query = sql
	// TODO: handle SQL params (interfaces)
	return tr
}

func (tr *TableReader) Partition(part, inParts int) *TableReader {
	tr.req.NumPartitions = inParts
	tr.req.PartitionIdx = part
	return tr
}

func (tr *TableReader) Load() (requests.ReadResponse, error) {

	if tr.executed {
		return nil, fmt.Errorf("Request was already executed")
	}
	tr.executed = true

	tr.req.Fullpath = tr.dc.GetFullpath(tr.datasource, tr.path)
	tr.dc.Logger.DebugWith("Table load", "req", tr.req)
	return tr.datasource.TableReadReq(tr.req)
}


