package databinding

import (
	"testing"
	"github.com/nuclio/zap"
	"fmt"
	"github.com/yaronha/databinding/datasources"
)



func TestName(t *testing.T) {


	logger, err := nucliozap.NewNuclioZapCmd("v3test", nucliozap.WarnLevel)
	if err != nil {
		t.Fatal("Failed to create logger", err )
	}

	some_config := map[string]datasources.DataSourceCfg{
		"db0": {Class:"v3io", URL:"", Resource:"nuclio", BasePath:""},
	}

	dc := NewDataContext(logger, some_config)

	req, _ := dc.Table.Write("db0://cars").ToKeys("3").WithExpression("model='%s'", "Nisan").Do()
	fmt.Println(req)
	rows, _ := dc.Table.Read("db0://cars").Load()
	for rows.Next() {
		fmt.Println(rows.Columns())
	}


}
