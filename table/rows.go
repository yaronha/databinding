package table

import (
	"strings"
	"fmt"
	"strconv"
)

type tableRow map[string]interface{}



func (tr tableRow) Scan(fields string, pointers ...interface{}) error {
	list := strings.Split(fields, ",")
	if len(list) != len(pointers) {
		return fmt.Errorf("number of fields (comma seperated) must match number of pointers)")
	}
	for idx, name := range list {
		field, ok := tr[name]
		if !ok {
			field = ""
		}
		p := pointers[idx]
		switch p.(type) {
		case *string:
			*p.(*string) = AsString(field)
		case *int:
			*p.(*int) = AsInt(field)

		}

	}
	return nil
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

