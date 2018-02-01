package v3io

import (
	"github.com/yaronha/databinding/requests"
	"github.com/v3io/v3io-go-http"
	"fmt"
)

func (v *v3ioDS) TableWriteReq(req *requests.WriteRequest) (requests.ExecResponse, error) {
	newResp := v3ioExecResponse{}

	if req.Expression != "" && len(req.Fields)>0 {
		return &newResp, fmt.Errorf("Need to choose between Expression and Fields, cannot use both")
	}
	if req.Expression == "" && len(req.Fields)==0 {
		return &newResp, fmt.Errorf("Nothing to update, both Expression and Fields are empty")
	}

	var err error
	var resp *v3io.Request
	if req.WaitGroup == 0 {
		newResp.respChan = make(chan *v3io.Response, 1)
	} else {

	}

	// TODO: multiple keys & to string conversion
	fullpath := req.Fullpath + "/" + req.Keys[0].(string)
	if len(req.Fields)>0 {
		resp , err = v.container.UpdateItem(&v3io.UpdateItemInput{
			Path: fullpath, Attributes: req.Fields}, newResp.respChan)

	} else {
		resp , err = v.container.UpdateItem(&v3io.UpdateItemInput{
			Path: fullpath, Expression: &req.Expression}, newResp.respChan)
	}

	if err != nil {
		v.logger.ErrorWith("Failed to submit write request", "path", fullpath, "err", err)
	}

	newResp.Err = err
	newResp.Id = resp.ID
	newResp.Req = req
	return &newResp, err
}

type v3ioExecResponse struct {
	requests.AbstractExecResponse
	respChan     chan *v3io.Response
}

func (r *v3ioExecResponse) Result() (interface{}, error) {
	resp := <- r.respChan
	r.Err = resp.Error
	if resp.Error != nil {
		r.Logger.ErrorWith("Failed request", "path", r.Req.Fullpath, "err", resp.Error)
	}
	return resp, resp.Error
}

