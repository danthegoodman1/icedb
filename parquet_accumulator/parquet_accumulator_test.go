package parquet_accumulator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/danthegoodman1/gojsonutils"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"testing"
)

func TestGetSchemaString(t *testing.T) {
	a := NewParquetAccumulator()
	a.WriteRow(map[string]any{
		"colA": "hey",
	})
	a.WriteRow(map[string]any{
		"colB": 1.2,
	})
	a.WriteRow(map[string]any{
		"colC": []any{"hey"},
	})

	a.WriteRow(map[string]any{
		"colA": "hey",
		"colB": 1,
	})

	a.WriteRow(map[string]any{
		"colC": []any{"hey"},
		"colB": 1.2,
	})

	schemaString, err := a.GetSchemaString()
	if err != nil {
		t.Fatal(err)
	}
	if schemaString != `{"Tag":"name=parquet_go_root, repetitiontype=REQUIRED","Fields":[{"Tag":"type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, name=colA, repetitiontype=OPTIONAL"},{"Tag":"type=DOUBLE, name=colB, repetitiontype=OPTIONAL"},{"Tag":"type=LIST, name=colC, repetitiontype=OPTIONAL","Fields":[{"Tag":"type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, name=Element, repetitiontype=OPTIONAL"}]}]}` {
		t.Log(schemaString)
		t.Fatal("got incorrect schema string")
	}

}

func TestFullCycle(t *testing.T) {
	jsonMap := map[string]any{
		"colA": map[string]any{
			"a": "hey",
			"b": 2,
		},
		"colC": []any{"hey"},
		"colB": 1.2,
	}
	flat, err := gojsonutils.Flatten(jsonMap, nil)
	if err != nil {
		t.Fatal("error flattening JSON map")
	}
	flatMap, ok := flat.(map[string]any)
	if !ok {
		t.Fatal(fmt.Sprintf("got a non flat map: %+v", flat))
	}
	psa := NewParquetAccumulator()
	psa.WriteRow(flatMap)

	t.Log("cols", psa.GetColumns())

	parquetSchema, err := psa.GetSchemaString()
	if err != nil {
		t.Fatal("error in GetSchemaString")
	}

	f, err := os.Create("temp.parquet")
	if err != nil {
		t.Fatal(err)
	}

	t.Log(parquetSchema)
	pw, err := writer.NewJSONWriterFromWriter(parquetSchema, f, 4)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(flatMap)

	b, err := json.Marshal(flatMap)
	if err != nil {
		t.Fatal("error in json.Marshal: %w", err)
	}

	err = pw.Write(string(b))
	if err != nil {
		t.Fatal(err)
	}

	err = pw.WriteStop()
	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	fr, err := local.NewLocalFileReader("temp.parquet")
	if err != nil {
		t.Fatal("Can't open file", err)
	}

	pr, err := reader.NewParquetReader(fr, parquetSchema, 4)
	if err != nil {
		t.Fatal("Can't create parquet reader", err)
		return
	}

	num := int(pr.GetNumRows())
	t.Log("rows", num)
	//for i := 0; i < num; i++ {
	//	stus := make(map[string]any)
	//	if err = pr.Read(&stus); err != nil {
	//		t.Fatal("Read error", err)
	//	}
	//	t.Log("got", stus)
	//}

	res, err := pr.ReadByNumber(num)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range res {
		// item is a struct
		b, err := json.Marshal(item)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%+v", string(b))
	}

	pr.ReadStop()
	fr.Close()
}

func TestS3Reader(t *testing.T) {
	// TEMP
	r, _ := s3v2.NewS3FileReaderWithParams(context.Background(), s3v2.S3FileReaderParams{
		Bucket:         "",
		Key:            "",
		S3Client:       nil,
		Version:        nil,
		MinRequestSize: 0,
	})
	r = r
}