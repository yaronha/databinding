package v3io

import (
	"github.com/v3io/v3io-go-http"
	"github.com/pkg/errors"
	"github.com/yaronha/databinding/requests"
)

type V3ioItemsCursor struct {
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
func (ic *V3ioItemsCursor) Next() (*requests.Item, error) {

	// are there any more items left in the previous response we received?
	if ic.itemIndex < len(ic.items) {
		item := requests.Item(ic.items[ic.itemIndex])

		// next time we'll give next item
		ic.itemIndex++

		return &item, nil
	}

	// are there any more items up stream?
	if !ic.moreItemsExist {
		return nil, nil
	}

	// get the previous request input and modify it with the marker
	ic.input.Marker = ic.nextMarker

	// invoke get items
	newResponse, err := ic.container.Sync.GetItems(ic.input)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to request next items")
	}

	// release the previous response
	ic.response.Release()

	// set the new response - read all the sub information from it
	ic.setResponse(newResponse)

	// and recurse into next now that we repopulated response
	return ic.Next()
}

// gets all items
func (ic *V3ioItemsCursor) GetAll() ([]interface{}, error) {
	items := []interface{}{}

	for {
		item, err := ic.Next()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get next item")
		}

		if item == nil {
			break
		}

		items = append(items, item)
	}

	return items, nil
}

func (ic *V3ioItemsCursor) setResponse(response *v3io.Response) {
	ic.response = response

	getItemsOutput := response.Output.(*v3io.GetItemsOutput)

	ic.moreItemsExist = !getItemsOutput.Last
	ic.nextMarker = getItemsOutput.NextMarker
	ic.items = getItemsOutput.Items
	ic.itemIndex = 0
}


