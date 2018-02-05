package v3io

import (
	"github.com/v3io/v3io-go-http"
	"github.com/pkg/errors"
	"github.com/yaronha/databinding/requests"
	"encoding/binary"
	"math"
)

type BaseV3ioItemsCursor struct {
	currentItem    *map[string]interface{}
	lastError      error
}

func (ic *BaseV3ioItemsCursor) Error() error {
	return ic.lastError
}

func (ic *BaseV3ioItemsCursor) GetFields() *map[string]interface{} {
	return ic.currentItem
}

func (ic *BaseV3ioItemsCursor) GetField(name string) requests.TableFieldTypes {
	f := (*ic.currentItem)[name]

	return &V3ioFieldTypes{AbstractTableField:requests.AbstractTableField{f}}
}

func (ic *BaseV3ioItemsCursor) Scan(fields []string, dest ...interface{}) error {

	for idx, name := range fields {
		field, ok := (*ic.currentItem)[name]
		if !ok {
			field = ""
		}
		p := dest[idx]
		switch p.(type) {
		case *[]byte:
			*p.(*[]byte) = requests.AsBytes(field)
		case *string:
			*p.(*string) = requests.AsString(field)
		case *int:
			*p.(*int) = requests.AsInt(field)
		}
	}
	return nil
}

type V3ioFieldTypes struct {
	requests.AbstractTableField

}

func (f *V3ioFieldTypes) AsInt64Array() []uint64 {
	var array []uint64
	switch f.Val.(type) {
	case []byte:
		bytes := f.Val.([]byte)
		for i :=16 ; i+8 <= len(bytes); i += 8 {
			val := binary.LittleEndian.Uint64(bytes[i:i+8])
			array = append(array, val)
		}
	}
	return array
}

func (f *V3ioFieldTypes) AsFloat64Array() []float64 {
	var array []float64

	switch f.Val.(type) {
	case []byte:
		bytes := f.Val.([]byte)
		for i :=16 ; i+8 <= len(bytes); i += 8 {
			val := binary.LittleEndian.Uint64(bytes[i:i+8])
			float := math.Float64frombits(val)
			array = append(array, float)
		}
	}
	return array
}



type V3ioItemsCursor struct {
	BaseV3ioItemsCursor
	nextMarker     string
	moreItemsExist bool
	itemIndex      int
	items          []v3io.Item
	response       *v3io.Response
	input          *v3io.GetItemsInput
	container      *v3io.Container
}

func newItemsCursor(container *v3io.Container, input *v3io.GetItemsInput, response *v3io.Response) requests.ItemsCursor {
	newItemsCursor := &V3ioItemsCursor{
		container: container,
		input:     input,
	}

	newItemsCursor.setResponse(response)

	return newItemsCursor
}

// release a cursor and its underlying resources
func (ic *V3ioItemsCursor) Release() {
	ic.response.Release()
}

// get the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *V3ioItemsCursor) Next() bool {

	// are there any more items left in the previous response we received?
	if ic.itemIndex < len(ic.items) {
		item := map[string]interface{}(ic.items[ic.itemIndex])
		ic.currentItem = &item

		// next time we'll give next item
		ic.itemIndex++
		ic.lastError = nil

		return true
	}

	// are there any more items up stream?
	if !ic.moreItemsExist {
		ic.currentItem = nil
		return false
	}

	// get the previous request input and modify it with the marker
	ic.input.Marker = ic.nextMarker

	// invoke get items
	newResponse, err := ic.container.Sync.GetItems(ic.input)
	if err != nil {
		ic.lastError = errors.Wrap(err, "Failed to request next items")
		ic.currentItem = nil
		return false
	}

	// release the previous response
	ic.response.Release()

	// set the new response - read all the sub information from it
	ic.setResponse(newResponse)

	// and recurse into next now that we repopulated response
	return ic.Next()
}


func (ic *V3ioItemsCursor) setResponse(response *v3io.Response) {
	ic.response = response

	getItemsOutput := response.Output.(*v3io.GetItemsOutput)

	ic.moreItemsExist = !getItemsOutput.Last
	ic.nextMarker = getItemsOutput.NextMarker
	ic.items = getItemsOutput.Items
	ic.itemIndex = 0
}


type V3ioByKeyCursor struct {
	BaseV3ioItemsCursor
	moreItemsExist bool
	itemIndex      int
	respMap        map[uint64]string
	respChan       chan *v3io.Response
	container      *v3io.Container
}

func newByKeyCursor(container *v3io.Container, respMap map[uint64]string, respChan chan *v3io.Response) requests.ItemsCursor {
	newByKeyCursor := &V3ioByKeyCursor{
		container: container,
		respMap: respMap,
		respChan: respChan,
	}

	return newByKeyCursor
}

func (ic *V3ioByKeyCursor) Next() bool {

	if len(ic.respMap) == 0 {
		ic.currentItem = nil
		return false
	}

	response := <- ic.respChan

	key := ic.respMap[response.ID]
	if response.Error != nil {
		ic.lastError = response.Error
		delete(ic.respMap, response.ID)
		return false
	}

	item := map[string]interface{}(response.Output.(*v3io.GetItemOutput).Item)
	item["__name"] = key
	ic.currentItem = &item
	ic.lastError = nil
	delete(ic.respMap, response.ID)
	response.Release()

	return true
}

func (ic *V3ioByKeyCursor) Release() {

}

