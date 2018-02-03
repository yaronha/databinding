package table

import (
	"github.com/yaronha/databinding/requests"
	"github.com/yaronha/databinding/datactx"
	"fmt"
)

func NewWriter(dc *datactx.DataContextCfg, path string) *TableWriter {
	newWriter := TableWriter{dc: dc, path:path}
	newWriter.req = &requests.WriteRequest{}
	return &newWriter
}

type TableWriter struct {
	dc         *datactx.DataContextCfg
	req        *requests.WriteRequest
	path       string
	executed   bool
}

func (tw *TableWriter) Format(format string) *TableWriter {
	tw.req.Format = format
	return tw
}

func (tw *TableWriter) ToKeys(keys ...interface{}) *TableWriter {
	tw.req.Keys = keys
	return tw
}

func (tw *TableWriter) WithExpression(expr string, attributes ...interface{}) *TableWriter {
	tw.req.Expression = fmt.Sprintf(expr, attributes...)
	return tw
}

func (tw *TableWriter) WithFields(fields map[string]interface{}) *TableWriter {
	tw.req.Fields = fields
	return tw
}

func (tw *TableWriter) Condition(cond string) *TableWriter {
	tw.req.Condition = cond
	return tw
}

func (tw *TableWriter) DoAsync(wg int) (requests.ExecResponse, error) {

	if tw.executed {
		return nil, fmt.Errorf("Request was already executed")
	}
	tw.executed = true


	// TODO: verify that the data source support Table & Write methods
	ds, fullpath, err := tw.dc.GetSource(tw.path)
	if err != nil {
		return nil, err
	}

	tw.req.Fullpath = fullpath
	tw.req.WaitGroup = wg
	tw.dc.Logger.DebugWith("Table Write", "req", tw.req, "ds", ds.GetConfig())
	resp, err := ds.TableWriteReq(tw.req)
	if wg !=0 && err == nil {
		tw.dc.WaitGroups[wg] = append(tw.dc.WaitGroups[wg], resp)
	}
	return resp, err
}

func (tw *TableWriter) Do() (interface{}, error) {

	resp, err := tw.DoAsync(0)
	if err != nil {
		return nil, err
	}

	return resp.Result()
}


