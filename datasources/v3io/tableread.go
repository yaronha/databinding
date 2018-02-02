package v3io

import (
	"github.com/yaronha/databinding/requests"
	"strings"
	"github.com/v3io/v3io-go-http"
	"strconv"
)

func (v *v3ioDS) TableReadReq(req *requests.ReadRequest) (requests.ReadResponse, error) {

	// TODO: block more Loads, do pre-fetch, add GetItems

	readResp := v3ioReadResponse{}
	if len(req.Attributes) == 0 {
		req.Attributes = append(req.Attributes, "*")
	}
	readResp.Req = req
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
		readResp.Err = err
		return &readResp, err
	}

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
	if err != nil {
		return err
	}
	readResp.ItemsCurs = newItemsCursor(v.container, &input, response)
	return nil
}



type v3ioReadResponse struct {
	requests.AbstractReadResponse
	respChan     chan *v3io.Response
}




