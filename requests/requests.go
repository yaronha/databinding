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
	Query          string
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
	Fields() *map[string]interface{}
	Field(name string) TableFieldTypes
}

type AbstractReadResponse struct {
	Logger       logger.Logger
	Req          *ReadRequest
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
}

func (rs *AbstractReadResponse) Fields() *map[string]interface{} {
	return rs.ItemsCurs.GetFields()
}

func (rs *AbstractReadResponse) Field(name string) TableFieldTypes {
	return rs.ItemsCurs.GetField(name)
}

func (rs *AbstractReadResponse) Scan(fields string, dest ...interface{}) error {
	list := []string{}

	// fields can be nil/"", means return the fields based on the query order
	if fields != "" {
		list := strings.Split(fields, ",")
		if len(list) != len(dest) {
			return fmt.Errorf("number of fields (comma seperated) must match number of pointers)")
		}
	}

	return rs.ItemsCurs.Scan(list, dest)
}

func (rs *AbstractReadResponse) Scannn(fields string, dest ...interface{}) error {
	list := []string{}
	if len(list) != len(dest) {
		return fmt.Errorf("number of fields (comma seperated) must match number of pointers)")
	}
	for idx, name := range list {
		field, ok := (*rs.Fields())[name]
		if !ok {
			field = ""
		}
		p := dest[idx]
		switch p.(type) {
		case *[]byte:
			*p.(*[]byte) = asBytes(field)
		case *string:
			*p.(*string) = asString(field)
		case *int:
			*p.(*int) = asInt(field)

		}

	}
	return nil
}


type ItemsCursor interface {
	Release()
	Next() bool
	Error() error
	Scan(fields []string, dest ...interface{}) error
	GetFields() *map[string]interface{}
	GetField(name string) TableFieldTypes
}



type TableFieldTypes interface {
	AsInt() int
	AsStr() string
	AsBytes() []byte
	AsInterface() interface{}
	AsInt64Array() []uint64
	AsFloat64Array() []float64
}

type AbstractTableField struct {
	Val   interface{}
}

func (f *AbstractTableField) AsInt() int {
	return asInt(f.Val)
}

func (f *AbstractTableField) AsStr() string {
	return asString(f.Val)
}

func (f *AbstractTableField) AsBytes() []byte {
	if f.Val == nil {
		return nil
	}
	return asBytes(f.Val)
}

func (f *AbstractTableField) AsInterface() interface{} {
	return f.Val
}

func (f *AbstractTableField) AsInt64Array() []uint64 {
	val, ok := f.Val.([]uint64)
	if ok { return val }
	return []uint64{}
}

func (f *AbstractTableField) AsFloat64Array() []float64 {
	val, ok := f.Val.([]float64)
	if ok { return val }
	return []float64{}
}



func asInt(num interface{}) int {
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

