package table

import (
	"github.com/yaronha/databinding/requests"
	"github.com/yaronha/databinding/datactx"
	"fmt"
)


func NewReader(dc *datactx.DataContextCfg, path string, keys []interface{}) *TableReader {
	newReader := TableReader{dc: dc, path:path}
	newReader.req = &requests.ReadRequest{Keys:keys}
	return &newReader
}

type TableReader struct {
	dc         *datactx.DataContextCfg
	req        *requests.ReadRequest
	executed   bool
	path       string
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

func (tr *TableReader) Sql(sql string, vars ...interface{}) *TableReader {
	tr.req.SqlQuery = sql
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

	// TODO: verify that the data source support Table & Load methods
	ds, fullpath, err := tr.dc.GetSource(tr.path)
	if err != nil {
		return nil, err
	}

	tr.req.Fullpath = fullpath
	tr.dc.Logger.DebugWith("Table load", "req", tr.req, "ds", ds.GetConfig())
	return ds.TableReadReq(tr.req)
}


