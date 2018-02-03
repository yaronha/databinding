package databinding

import (
	"testing"
	"github.com/nuclio/zap"
	"fmt"
	"github.com/yaronha/databinding/datasources"
	"time"
)



func TestName(t *testing.T) {


	logger, err := nucliozap.NewNuclioZapCmd("v3test", nucliozap.InfoLevel)
	if err != nil {
		t.Fatal("Failed to create logger", err )
	}

	some_config := map[string]datasources.DataSourceCfg{
		"db0": {Class:"v3io", URL:"<TBD>", Resource:"nuclio", BasePath:""},
	}

	dc := NewDataContext(logger, some_config)

	dc.Table.Write("db0://cars").ToKeys("1").WithExpression("model='%s'", "AB").DoAsync(0)
	dc.Table.Write("db0://cars").ToKeys("3").WithExpression("model='%s'", "CD").DoAsync(0)
	resp, err := dc.Wait(0)
	logger.InfoWith("resp array", "resp", resp, "err", err)
	//rows, _ := dc.Table.Read("db0://cars").Load()
	rows, _ := dc.Table.Read("db0://cars", ).Load()
	for rows.Next() {
		logger.InfoWith("row", "cols", rows.Fields())
	}


}



func Test2(t *testing.T) {

	tm, err := time.Parse("2006-01-02 15:04:05","2015-01-06 13:05:07")
	fmt.Println(err, tm.Hour(), tm.Month(), tm.Minute(), tm.Second(), tm.Format("2006-01-02"))
	//os.Exit(0)


	logger, err := nucliozap.NewNuclioZapCmd("v3test", nucliozap.WarnLevel)
	if err != nil {
		t.Fatal("Failed to create logger", err )
	}

	some_config := map[string]datasources.DataSourceCfg{
		"db0": {Class:"v3io", URL:"<TBD>", Resource:"azureml", BasePath:""},
	}

	dc := NewDataContext(logger, some_config)

	dc.Table.Write("db0://dy-machines").ToKeys("3").WithExpression("model='%s'", "Nisan").DoAsync(1)
	dc.Wait(1)
	rows, _ := dc.Table.Read("db0://dy-machines/2018-01-29").Load()
	for rows.Next() {
		//rot := []byte{}
		//var id, val int
		//rows.Scan("machine_id,val,rotation", &id, &val, &rot)
		fmt.Println("rot:", rows.Field("rotate_samples").AsFloat64Array())
	}

}

