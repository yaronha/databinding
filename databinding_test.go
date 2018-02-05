package databinding

import (
	"testing"
	"fmt"
	"github.com/yaronha/databinding/datasources"
	"time"
	"github.com/v3io/v3io-go-http"
	"encoding/json"
)



func TestName(t *testing.T) {


	some_config := map[string]datasources.DataSourceCfg{
		"db0": {Class:"v3io", URL:"<TBD>", Resource:"nuclio", BasePath:""},
	}

	dc, _ := NewDataContext(some_config, false)

	container, err := dc.Raw("db0")
	fmt.Println(err)
	expr := "model='111'"
	err = container.(*v3io.Container).Sync.UpdateItem(&v3io.UpdateItemInput{
		Path: "cars/2", Expression: &expr })
	fmt.Println(err)

	dc.Table("db0").Write("cars").ToKeys("1").WithExpression("model='%s'", "xx").DoAsync(1)
	dc.Table("db0").Write("cars").ToKeys("3").WithExpression("model='%s'", "yy").DoAsync(1)
	resp, err := dc.Wait(1)
	dc.GetLogger().InfoWith("resp array", "resp", resp, "err", err)
	rows, _ := dc.Table("db0").Read("cars", ).Load()
	for rows.Next() {
		dc.GetLogger().InfoWith("row", "cols", rows.Fields())
	}


}

type machineStats struct {
	Name string
	Stats  map[string][]float64
}

var sensorNames = []string {"volt", "rotate", "pressure", "vibration"}

func Test2(t *testing.T) {

	tm, err := time.Parse("2006-01-02 15:04:05","2015-01-06 13:05:07")
	fmt.Println(err, tm.Hour(), tm.Month(), tm.Minute(), tm.Second(), tm.Format("2006-01-02"))
	//os.Exit(0)



	some_config := map[string]datasources.DataSourceCfg{
		"db0": {Class:"v3io", URL:"<TBD>", Resource:"azureml", BasePath:""},
	}

	dc, _ := NewDataContext(some_config, true)

	//dc.Table.Write("db0://dy-machines").ToKeys("3").WithExpression("model='%s'", "Nisan").DoAsync(1)
	//dc.Wait(1)
	resp := []machineStats{}
	rows, _ := dc.Table("db0").Read("dy-machines/2018-02-03").Load()
	for rows.Next() {
		mech := machineStats{}
		mech.Name = rows.Field("__name").AsStr()
		mech.Stats = map[string][]float64{}
		for _, name := range sensorNames {
			mech.Stats[name] = rows.Field(name + "_samples").AsFloat64Array()
		}
		resp = append(resp, mech)
	}
	body, _ := json.Marshal(resp)
	fmt.Println(string(body))

}

