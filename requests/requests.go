package requests

import (
	"github.com/nuclio/logger"
	"strconv"
	"strings"
	"fmt"
	"reflect"
)

type Item map[string]interface{}


type ReadRequest struct {
	Fullpath       string
	Keys           []interface{}
	Attributes     []string
	Filter         string
	Format         string
	SqlQuery       string
	NumPartitions  int
	PartitionIdx   int
	Schema         interface{}   // TODO:
}



type ReadResponse interface {
	// Err returns the error, if any, that was encountered during iteration.
	// Err may be called after an explicit or implicit Close.
	Error() error
	// Close closes the Reader, preventing further enumeration.
	// If Next is called and returns false and there are no further result sets, the Rows are closed automatically
	// and it will suffice to check the result of Err.
	Close() error
	// Next prepares the next result row for reading with the Scan or other read method.
	// It returns true on success, or false if there is no next result row or an error happened while preparing it.
	// Err should be consulted to distinguish between the two cases.
	// Every call to Scan, even the first one, must be preceded by a call to Next.
	Next() bool
	Scan(fields string, dest ...interface{}) error
	Columns() *Item
	Col(name string) TableField
}

type AbstractReadResponse struct {
	Logger       logger.Logger
	Req          *ReadRequest
	Loading      bool
	ItemsCurs    ItemsCursor
	Err          error

}


func (rs *AbstractReadResponse) Error() error {
	return rs.Err
}

func (rs *AbstractReadResponse) Close() error {
	return nil
}


func (rs *AbstractReadResponse) Next() bool {

	next := rs.ItemsCurs.Next()
	rs.Err = rs.ItemsCurs.Error()
	return next

/*

	if rs.Err !=nil {
		return false
	}

	// GetItems query case, 0-n results using iterator
	if len(rs.Req.Keys)==0 {
		// TODO: do a better imp
		item, err := rs.ItemsCurs.Next()
		if err != nil {
			rs.Err = err
			return false
		}

		// no more items (EOF)
		if item == nil {
			return false
		}

		rs.Data = append(rs.Data, item)
		rs.Cursor +=1
		return true
	}

	if len(rs.Data) <= rs.Cursor || len(rs.RespMap)>0 {
		//rs.getDataResp(1)
		// TODO: pre-fetch
	}

	if rs.Data == nil || len(rs.Data) <= rs.Cursor {
		return false
	}

	rs.Cursor +=1

	return true
*/
}

func (rs *AbstractReadResponse) Columns() *Item {
	return rs.ItemsCurs.GetItem()
}

func (rs *AbstractReadResponse) Col(name string) TableField {
	return &AbstractTableField{Resp:rs, Name:name}
}

func (rs *AbstractReadResponse) Scan(fields string, dest ...interface{}) error {
	list := strings.Split(fields, ",")
	if len(list) != len(dest) {
		return fmt.Errorf("number of fields (comma seperated) must match number of pointers)")
	}
	for idx, name := range list {
		field, ok := (*rs.Columns())[name]
		if !ok {
			field = ""
		}
		p := dest[idx]
		switch p.(type) {
		//		case *[]byte:
		//			*p.(*[]byte) = field.([]byte)
		case *string:
			*p.(*string) = AsString(field)
		case *int:
			*p.(*int) = AsInt(field)

		}

	}
	return nil
}


type ItemsCursor interface {
	Release()
	Next() bool
	Error() error
	GetItem() *Item
	//GetAll() ([]interface{}, error)
}



type TableField interface {
	AsInt() int
	AsStr() string
	AsBytes() []byte
}

type AbstractTableField struct {
	Resp   ReadResponse
	Name   string
}

func (f *AbstractTableField) AsInt() int {
	return AsInt((*f.Resp.Columns())[f.Name])
}

func (f *AbstractTableField) AsStr() string {
	return asString((*f.Resp.Columns())[f.Name])
}

func (f *AbstractTableField) AsBytes() []byte {
	if (*f.Resp.Columns())[f.Name] == nil {
		return nil
	}
	return asBytes((*f.Resp.Columns())[f.Name])
}



func AsInt(num interface{}) int {
	val, ok := num.(int)
	if ok { return val }
	return 0
}

func AsString(val interface{}) string {
	switch val.(type) {
	case string:
		return val.(string)
	case int:
		return strconv.Itoa(val.(int))
	}
	return ""
}


func asString(src interface{}) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case []uint64:
		list := []string{}
		for _, val := range src.([]uint64) {
			list = append(list, strconv.FormatUint(val, 10))
		}
		return strings.Join(list, ",")
	case []float64:
		list := []string{}
		for _, val := range src.([]float64) {
			list = append(list, strconv.FormatFloat(val, 'g', -1, 64))
		}
		return strings.Join(list, ",")
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", src)
}

func asBytes(src interface{}) []byte {
	switch v := src.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	}

	return []byte(asString(src))
}




type WriteRequest struct {
	Fullpath       string
	Keys           []interface{}
	Expression     string
	Fields         map[string]interface{}
	Format         string
	Condition      string
	WaitGroup      int
	Schema         interface{}   // TODO:
}


type ExecResponse interface {
	// Err returns the error, if any, that was encountered
	Error() error
	// Block until the write complete, for Async call.
	// return result in case there is one
	Result() (interface{}, error)
}

type AbstractExecResponse struct {
	Logger       logger.Logger
	Req          *WriteRequest
	Id           uint64
	Err          error
}

func (ar *AbstractExecResponse) Error() error {
	return ar.Err
}

func (ar *AbstractExecResponse) Result() (interface{}, error) {
	return nil, ar.Err
}

