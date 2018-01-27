package v3io

import (
	"github.com/yaronha/databinding/table"
	"github.com/yaronha/databinding/requests"
	"strings"
	"github.com/v3io/v3io-go-http"
	"strconv"
	"github.com/nuclio/logger"
	"fmt"
)

func (v *v3ioDS) TableReadReq(req *requests.ReadRequest) (requests.ReadResponse, error) {

	// TODO: block more Loads, do pre-fetch, add GetItems

	readResp := v3ioReadResponse{}
	if len(req.Attributes) == 0 {
		req.Attributes = append(req.Attributes, "*")
	}
	readResp.req = req
	keys := getKeys(req.Keys)
	v.logger.DebugWith("got read request", "req", req, "keys", keys)


	if len(req.Keys)==0 {
		// add "/" if missing
		if req.Fullpath != "" && !strings.HasSuffix(req.Fullpath, "/") {
			req.Fullpath = req.Fullpath +"/"
		}

		err := v.loadItems(&req.Fullpath, &req.Filter, &req.Attributes, &readResp)
		if err != nil {
			v.logger.ErrorWith("failed frames LoadAsync items","err",err, "path", req.Fullpath )
		}
		readResp.err = err
		return &readResp, err
	}

	readResp.respChan = make(chan *v3io.Response, len(keys))
	readResp.respMap  = map[uint64]string{}

	for _, key := range keys {
		resp, err := v.container.GetItem(&v3io.GetItemInput{
			Path: req.Fullpath +"/" + key, AttributeNames: req.Attributes}, readResp.respChan)
		if err != nil {
			readResp.err = err
			v.logger.ErrorWith("failed frames LoadAsync by key","err",err, "path", req.Fullpath +"/" + key)
		} else {
			readResp.respMap[resp.ID] = key
		}
	}

	return &readResp, nil
}

func getKeys(keys []interface{}) []string {
	if len(keys) == 0 {
		return []string{}
	}

	newKeys := []string{}
	for _, key := range keys {
		switch key.(type) {
		case string:
			newKeys = append(newKeys, key.(string))
		case int:
			newKeys = append(newKeys, strconv.Itoa(key.(int)))
		}

	}
	return newKeys
}

func (v *v3ioDS) loadItems(fullpath, filter *string, attrs *[]string, readResp *v3ioReadResponse) error {

	input := v3io.GetItemsInput{Path:*fullpath, AttributeNames:*attrs, Filter:*filter}

	response, err := v.container.Sync.GetItems(&input)
	//time.Sleep(time.Second)
	if err != nil {
		//ds.dc.logger.ErrorWith("Failed GetItems with:","error", err)
		return err
	}
	readResp.itemsCurs = newItemsCursor(v.container, &input, response)
	return nil
}



type v3ioReadResponse struct {
	logger       logger.Logger
	req          *requests.ReadRequest
	data         []*v3io.Item
	cursor       int
	loading      bool
	respChan     chan *v3io.Response
	respMap      map[uint64]string
	itemsCurs    *ItemsCursor
	err          error

}

func (rs *v3ioReadResponse) Err() error {
	return rs.err
}

func (rs *v3ioReadResponse) Close() error {
	return nil
}

func (rs *v3ioReadResponse) Next() bool {

	if rs.err !=nil {
		return false
	}

	// GetItems query case, 0-n results using iterator
	if len(rs.req.Keys)==0 {
		// TODO: do a better imp
		item, err := rs.itemsCurs.Next()
		if err != nil {
			rs.err = err
			return false
		}

		// no more items (EOF)
		if item == nil {
			return false
		}

		rs.data = append(rs.data, item)
		rs.cursor +=1
		return true
	}

	if len(rs.data) <= rs.cursor || len(rs.respMap)>0 {
		rs.getDataResp(1)
		// TODO: pre-fetch
	}

	if rs.data == nil || len(rs.data) <= rs.cursor {
		return false
	}

	rs.cursor +=1

	return true
}

func (rs *v3ioReadResponse) getDataResp(num int) error {
	submitted := len(rs.respMap)
	var err error
	if num > submitted {
		num = submitted
	}

	for numResponses := 0; numResponses < num; numResponses++ {
		response := <- rs.respChan

		key := rs.respMap[response.ID]
		if response.Error != nil {
			rs.err = response.Error
			rs.logger.ErrorWith("failed frames get resp","err",rs.err, "path", rs.req.Fullpath+"/"+key)
			err = rs.err
		} else {
			item := response.Output.(*v3io.GetItemOutput).Item
			item["__name"] = key
			rs.data = append(rs.data, &item)

		}
		delete(rs.respMap, response.ID)
	}

	return err
}

func (rs *v3ioReadResponse) getCurrentRow() *v3io.Item {
	if rs.cursor == 0 {
		return &v3io.Item{}
	}
	return rs.data[rs.cursor-1]
}

func (rs *v3ioReadResponse) Scan(fields string, dest ...interface{}) error {
	list := strings.Split(fields, ",")
	if len(list) != len(dest) {
		return fmt.Errorf("number of fields (comma seperated) must match number of pointers)")
	}
	for idx, name := range list {
		field, ok := (*rs.getCurrentRow())[name]
		if !ok {
			field = ""
		}
		p := dest[idx]
		switch p.(type) {
		case *string:
			*p.(*string) = table.AsString(field)
		case *int:
			*p.(*int) = table.AsInt(field)

		}

	}
	return nil
}

func (rs *v3ioReadResponse) Columns() map[string]interface{} {
	var row map[string]interface{}
	row = *rs.getCurrentRow()
	return row
}

func (rs *v3ioReadResponse) Col(name string) requests.TableField {
	return &v3ioTableField{rs:rs, name:name}
}

type v3ioTableField struct {
	rs    *v3ioReadResponse
	name  string
}

func (f *v3ioTableField) AsInt() int {
	return table.AsInt((*f.rs.getCurrentRow())[f.name])
}

func (f *v3ioTableField) AsStr() string {
	return table.AsString((*f.rs.getCurrentRow())[f.name])
}

