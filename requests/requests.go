package requests



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
	Err() error
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
	Columns() map[string]interface{}
	Col(name string) TableField
}



type TableField interface {
	AsInt() int
	AsStr() string
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
	Err() error
	// Block until the write complete, for Async call.
	// return result in case there is one
	Result() (interface{}, error)
}

