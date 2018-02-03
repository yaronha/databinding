package v3io

import (
	"github.com/yaronha/databinding/requests"
	"strings"
	"github.com/v3io/v3io-go-http"
	"strconv"
)

func (v *v3ioDS) TableReadReq(req *requests.ReadRequest) (requests.ReadResponse, error) {

	readResp := requests.AbstractReadResponse{}
	if len(req.Attributes) == 0 {
		req.Attributes = append(req.Attributes, "*")
	}
	readResp.Req = req
	keys := toStringKeys(req.Keys)
	v.logger.DebugWith("got read request", "req", req, "keys", keys)

	if len(req.Keys)==0 {
		// use GetItems query
		err := v.loadItems(req, &readResp)
		if err != nil {
			v.logger.ErrorWith("failed frames LoadAsync items","err",err, "path", req.Fullpath )
		}
		readResp.Err = err
		return &readResp, err
	}

	// Use multiple async GetItem calls, one per key
	respChan := make(chan *v3io.Response, len(keys))
	respMap  := map[uint64]string{}
	readResp.Logger = v.logger

	for _, key := range keys {
		resp, err := v.container.GetItem(&v3io.GetItemInput{
			Path: req.Fullpath +"/" + key, AttributeNames: req.Attributes}, respChan)
		if err != nil {
			readResp.Err = err
			v.logger.ErrorWith("failed frames LoadAsync by key","err",err, "path", req.Fullpath +"/" + key)
			// TODO: release all chan resp...
		} else {
			respMap[resp.ID] = key
		}
	}

	ic := newByKeyCursor(v.container, respMap, respChan)
	readResp.ItemsCurs = ic
	return &readResp, nil
}

func toStringKeys(keys []interface{}) []string {
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

func (v *v3ioDS) loadItems(req *requests.ReadRequest, readResp *requests.AbstractReadResponse) error {

	// add "/" if missing
	fullpath := req.Fullpath
	if req.Fullpath != "" && !strings.HasSuffix(req.Fullpath, "/") {
		fullpath = req.Fullpath +"/"
	}

	input := v3io.GetItemsInput{Path:fullpath, AttributeNames:req.Attributes, Filter:req.Filter}

	response, err := v.container.Sync.GetItems(&input)
	if err != nil {
		return err
	}
	readResp.ItemsCurs = newItemsCursor(v.container, &input, response)
	return nil
}




