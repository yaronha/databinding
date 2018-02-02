package databinding

import (
	"testing"
	"github.com/nuclio/zap"
	"fmt"
	"github.com/yaronha/databinding/datasources"
	"encoding/binary"
	"math"
	"time"
	"os"
)



func TestName(t *testing.T) {


	logger, err := nucliozap.NewNuclioZapCmd("v3test", nucliozap.InfoLevel)
	if err != nil {
		t.Fatal("Failed to create logger", err )
	}

	some_config := map[string]datasources.DataSourceCfg{
		"db0": {Class:"v3io", URL:"199.19.70.139:8081", Resource:"nuclio", BasePath:""},
	}

	dc := NewDataContext(logger, some_config)

	req, _ := dc.Table.Write("db0://cars").ToKeys("3").WithExpression("model='%s'", "Kuku").Do()
	fmt.Println(req)
	//rows, _ := dc.Table.Read("db0://cars").Load()
	rows, _ := dc.Table.Read("db0://cars", 1,3).Load()
	for rows.Next() {
		logger.InfoWith("row", "cols", rows.Columns())
	}


}

func Float64frombytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	fmt.Println(bits, float)
	return float
}

func BytesToInt64Array(bytes []byte) []uint64 {
	var array []uint64
	for i :=16 ; i+8 < len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i:i+8])
		array = append(array, val)
	}
	fmt.Println(array)
	return array
}

func BytesToFloat64Array(bytes []byte) []float64 {
	var array []float64
	for i :=16 ; i+8 < len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i:i+8])
		float := math.Float64frombits(val)
		array = append(array, float)
	}
	fmt.Println(array)
	return array
}

func Test2(t *testing.T) {

	tm, err := time.Parse("2006-01-02 15:04:05","2015-01-06 13:05:07")
	fmt.Println(err, tm.Hour(), tm.Month(), tm.Minute(), tm.Second(), tm.Format("2006-01-02"))
	os.Exit(0)


	logger, err := nucliozap.NewNuclioZapCmd("v3test", nucliozap.WarnLevel)
	if err != nil {
		t.Fatal("Failed to create logger", err )
	}

	some_config := map[string]datasources.DataSourceCfg{
		"db0": {Class:"v3io", URL:"199.19.70.139:8081", Resource:"nuclio", BasePath:""},
	}

	dc := NewDataContext(logger, some_config)

	dc.Table.Write("db0://cars").ToKeys("3").WithExpression("model='%s'", "Nisan").Do()
	rows, _ := dc.Table.Read("db0://cars").Load()
	for rows.Next() {
		//rot := []byte{}
		//var id, val int
		//rows.Scan("machine_id,val,rotation", &id, &val, &rot)
		ma := rows.Col("my_array").AsBytes()
		rt := rows.Col("rotation_sum").AsBytes()
		fmt.Println("rot:", rows.Col("__name").AsStr(),BytesToFloat64Array(rt), BytesToInt64Array(ma))
	}

}

//fmt.Println(req)
