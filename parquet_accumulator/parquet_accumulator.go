package parquet_accumulator

import (
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
	"strings"
)

type (
	ParquetSchemaAccumulator struct {
		Schema ParquetSchema
	}

	ParquetSchema struct {
		TagStructs SchemaTag        `json:",omitempty"`
		Fields     []*ParquetSchema `json:",omitempty"`
	}

	ParquetJSONSchema struct {
		Tag    string               `json:",omitempty"`
		Fields []*ParquetJSONSchema `json:",omitempty"`
	}

	SchemaTag struct {
		Name           string         `json:"name,omitempty"`
		Type           string         `json:"type,omitempty"`
		ConvertedType  string         `json:"convertedtype,omitempty"`
		RepetitionType RepetitionType `json:"repetitiontype,omitempty"`
		Encoding       string         `json:"encoding,omitempty"`
	}

	RepetitionType string

	MergeParquetJSONSchema struct {
		Tag    string                   `json:",omitempty"`
		Fields []*parquet.SchemaElement `json:",omitempty"`
	}
)

var (
	Optional RepetitionType = "OPTIONAL"
	Required RepetitionType = "REQUIRED"
)

func NewParquetAccumulator() ParquetSchemaAccumulator {
	return ParquetSchemaAccumulator{
		Schema: ParquetSchema{
			TagStructs: SchemaTag{
				Name:           "parquet_go_root",
				RepetitionType: Required,
			},
		},
	}
}

func (pa *ParquetSchemaAccumulator) WriteRow(row map[string]any) {
	// Accumulate the Schema
	for key, val := range row {
		if pa.fieldExists(key) {
			continue
		}
		rowSchema := pa.getParquetSchema(key, val)
		if rowSchema != nil {
			pa.Schema.Fields = append(pa.Schema.Fields, rowSchema)
		}
	}
}

// getParquetTypes returns the Type and ConvertedType
func (pa *ParquetSchemaAccumulator) getParquetSchema(key string, item any) *ParquetSchema {
	schema := &ParquetSchema{
		TagStructs: SchemaTag{
			Name:           strings.ToUpper(key[:1]) + key[1:], // it can figure this out
			RepetitionType: Optional,
		},
	}
	reflectType := reflect.TypeOf(item)
	if reflectType.Kind() == reflect.Ptr {
		reflectType = reflectType.Elem()
	}

	if reflectType.Kind() == reflect.Slice {
		val := reflect.ValueOf(item)

		itemLength := val.Len()
		var nonNilVal any = nil
		for i := 0; i < itemLength; i++ {
			if !val.IsNil() {
				nonNilVal = val.Index(i)
				break
			}
		}
		if nonNilVal == nil {
			return nil
		}
		schema.TagStructs.Type = "LIST"
		schema.Fields = append(schema.Fields, pa.getParquetSchema("Element", val.Index(0).Interface()))
	} else if _, isStr := item.(string); isStr {
		schema.TagStructs.Type = "BYTE_ARRAY"
		schema.TagStructs.ConvertedType = "UTF8"
		schema.TagStructs.Encoding = "PLAIN"
	} else if _, isStr := item.(*string); isStr {
		schema.TagStructs.Type = "BYTE_ARRAY"
		schema.TagStructs.ConvertedType = "UTF8"
		schema.TagStructs.Encoding = "PLAIN"
	} else {
		// Float otherwise since we can't tell the difference in JSON
		schema.TagStructs.Type = "DOUBLE"
	}

	return schema
}

func (pa *ParquetSchemaAccumulator) fieldExists(fieldName string) (exists bool) {
	for _, field := range pa.Schema.Fields {
		if field.TagStructs.Name == fieldName {
			return true
		}
	}
	return
}

func (pa *ParquetSchemaAccumulator) GetColumnNames() []string {
	var cols []string
	for _, field := range pa.Schema.Fields {
		cols = append(cols, field.TagStructs.Name)
	}
	return cols
}

func (ps *ParquetSchema) GetType() string {
	switch ps.TagStructs.Type {
	case "BYTE_ARRAY":
		return "string"
	case "DOUBLE":
		return "float"
	case "LIST":
		return fmt.Sprintf("list(%s)", ps.Fields[0].GetType())
	default:
		// This should not be possible, don't want to do massive error checking though
		// I can already hear Rust elitists yelling at me
		return "I AM BROKEN AH"
	}
}

// GetColumnTypes returns the types of columns in the same order, either `string`, `float`, or `list(x)` (recursive0
func (pa *ParquetSchemaAccumulator) GetColumnTypes() []string {
	var cols []string
	for _, field := range pa.Schema.Fields {
		cols = append(cols, field.GetType())
	}
	return cols
}

// ToParquetJSONSchema recursively converts
func (ps *ParquetSchema) ToParquetJSONSchema() *ParquetJSONSchema {
	var tagArr []string
	if ps.TagStructs.Type != "" {
		tagArr = append(tagArr, "type="+ps.TagStructs.Type)
	}
	if ps.TagStructs.ConvertedType != "" {
		tagArr = append(tagArr, "convertedtype="+ps.TagStructs.ConvertedType)
	}
	if ps.TagStructs.Encoding != "" {
		tagArr = append(tagArr, "encoding="+ps.TagStructs.Encoding)
	}
	if ps.TagStructs.Name != "" {
		tagArr = append(tagArr, "name="+ps.TagStructs.Name)
	}
	if string(ps.TagStructs.RepetitionType) != "" {
		tagArr = append(tagArr, "repetitiontype="+string(ps.TagStructs.RepetitionType))
	}
	var fields []*ParquetJSONSchema
	for _, field := range ps.Fields {
		fields = append(fields, field.ToParquetJSONSchema())
	}
	return &ParquetJSONSchema{
		Tag:    strings.Join(tagArr, ", "),
		Fields: fields,
	}
}

// GetSchemaString returns the JSON formatted Schema string
func (pa *ParquetSchemaAccumulator) GetSchemaString() (string, error) {
	// Generate the Tag string
	var fields []*ParquetJSONSchema
	for _, field := range pa.Schema.Fields {
		fields = append(fields, field.ToParquetJSONSchema())
	}
	pjs := ParquetJSONSchema{
		Tag:    "name=parquet_go_root, repetitiontype=REQUIRED",
		Fields: fields,
	}

	b, err := json.Marshal(pjs)
	if err != nil {
		return "", fmt.Errorf("error in json.Marshal: %w", err)
	}
	return string(b), nil
}

//// ToParquetJSONSchema recursively converts
//func (ps *MergeParquetJSONSchema) toParquetJSONSchema(element *parquet.SchemaElement) *ParquetJSONSchema {
//	var tagArr []string
//	if element.Type != nil {
//		tagArr = append(tagArr, fmt.Sprintf("type=%s", element.Type))
//	}
//	if element.ConvertedType != nil {
//		tagArr = append(tagArr, fmt.Sprintf("convertedtype=%s", element.ConvertedType))
//	}
//	//if element.Encoding != nil {
//	//	tagArr = append(tagArr, fmt.Sprintf("encoding=%s", element.Encoding))
//	//}
//	if element.Name != "" {
//		tagArr = append(tagArr, fmt.Sprintf("name=%s", element.Name))
//	}
//	if element.RepetitionType != nil {
//		tagArr = append(tagArr, fmt.Sprintf("repetitiontype=%s", element.RepetitionType))
//	}
//	var fields []*ParquetJSONSchema
//	for _, field := range ps.Fields {
//		fields = append(fields, ps.toParquetJSONSchema(field))
//	}
//	return &ParquetJSONSchema{
//		Tag:    strings.Join(tagArr, ", "),
//		Fields: fields,
//	}
//}
//
//func (mps *MergeParquetJSONSchema) toParquetSchema() *ParquetSchema {
//	ps := ParquetSchema{
//		TagStructs: SchemaTag{
//			Name:           "parquet_go_root",
//			RepetitionType: Required,
//		},
//	}
//	for _, field := range mps.Fields {
//		field.
//			fields = append(fields, pa.toParquetJSONSchema(field))
//	}
//}
//
//// GetSchemaString returns the JSON formatted Schema string
//func (pa *MergeParquetJSONSchema) GetSchemaString() (string, error) {
//	// Generate the Tag string
//	var fields []*ParquetJSONSchema
//	fmt.Printf("%+v", pa)
//	for _, field := range pa.Fields {
//		fields = append(fields, pa.toParquetJSONSchema(field))
//	}
//	pjs := ParquetJSONSchema{
//		Tag:    "name=parquet_go_root, repetitiontype=REQUIRED",
//		Fields: fields,
//	}
//
//	b, err := json.Marshal(pjs)
//	if err != nil {
//		return "", fmt.Errorf("error in json.Marshal: %w", err)
//	}
//	return string(b), nil
//}
//
//func (pa *MergeParquetJSONSchema) GetColumnNames() []string {
//	var cols []string
//	for _, field := range pa.Fields {
//		cols = append(cols, field.Name)
//	}
//	return cols
//}
