package mapping

import (
	"errors"
	"fmt"
	"goposm/element"
	"goposm/logging"
	"regexp"
	"strconv"
	"strings"
)

var log = logging.NewLogger("mapping")

var AvailableFieldTypes map[string]FieldType

func init() {
	AvailableFieldTypes = map[string]FieldType{
		"bool":                 {"bool", "bool", Bool, nil},
		"boolint":              {"boolint", "int8", BoolInt, nil},
		"id":                   {"id", "int64", Id, nil},
		"string":               {"string", "string", String, nil},
		"direction":            {"direction", "int8", Direction, nil},
		"integer":              {"integer", "int32", Integer, nil},
		"mapping_key":          {"mapping_key", "string", Key, nil},
		"mapping_value":        {"mapping_value", "string", Value, nil},
		"geometry":             {"geometry", "geometry", Geometry, nil},
		"wayzorder":            {"wayzorder", "int32", WayZOrder, nil},
		"pseudoarea":           {"pseudoarea", "float32", PseudoArea, nil},
		"zorder":               {"zorder", "int32", nil, MakeZOrder},
		"string_suffixreplace": {"string_suffixreplace", "string", nil, MakeSuffixReplace},
	}
}

type MakeValue func(string, *element.OSMElem, Match) interface{}

type MakeMakeValue func(string, FieldType, Field) (MakeValue, error)

type FieldSpec struct {
	Name string
	Type FieldType
}

func (f *FieldSpec) Value(elem *element.OSMElem, match Match) interface{} {
	if f.Type.Func != nil {
		return f.Type.Func(elem.Tags[f.Name], elem, match)
	}
	return nil
}

type TableFields struct {
	fields []FieldSpec
}

func (t *TableFields) MakeRow(elem *element.OSMElem, match Match) []interface{} {
	var row []interface{}
	for _, field := range t.fields {
		row = append(row, field.Value(elem, match))
	}
	return row
}

func (field *Field) FieldType() *FieldType {
	if fieldType, ok := AvailableFieldTypes[field.Type]; ok {
		if fieldType.MakeFunc != nil {
			makeValue, err := fieldType.MakeFunc(field.Name, fieldType, *field)
			if err != nil {
				log.Print(err)
			}
			fieldType = FieldType{fieldType.Name, fieldType.GoType, makeValue, nil}
		}
		return &fieldType
	}
	return nil
}

func (t *Table) TableFields() *TableFields {
	result := TableFields{}

	for _, mappingField := range t.Fields {
		field := FieldSpec{}
		field.Name = mappingField.Name

		fieldType := mappingField.FieldType()
		if fieldType != nil {
			field.Type = *fieldType
		} else {
			log.Warn("unhandled type:", mappingField.Type)
		}
		result.fields = append(result.fields, field)
	}
	return &result
}

type FieldType struct {
	Name     string
	GoType   string
	Func     MakeValue
	MakeFunc MakeMakeValue
}

func Bool(val string, elem *element.OSMElem, match Match) interface{} {
	if val == "" || val == "0" || val == "false" || val == "no" {
		return false
	}
	return true
}

func BoolInt(val string, elem *element.OSMElem, match Match) interface{} {
	if val == "" || val == "0" || val == "false" || val == "no" {
		return 0
	}
	return 1
}

func String(val string, elem *element.OSMElem, match Match) interface{} {
	return val
}

func Integer(val string, elem *element.OSMElem, match Match) interface{} {
	v, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return nil
	}
	return v
}

func Id(val string, elem *element.OSMElem, match Match) interface{} {
	return elem.Id
}

func Key(val string, elem *element.OSMElem, match Match) interface{} {
	return match.Key
}

func Value(val string, elem *element.OSMElem, match Match) interface{} {
	return match.Value
}

func Direction(val string, elem *element.OSMElem, match Match) interface{} {
	if val == "1" || val == "yes" || val == "true" {
		return 1
	} else if val == "-1" {
		return -1
	} else {
		return 0
	}
}

func Geometry(val string, elem *element.OSMElem, match Match) interface{} {
	return elem.Geom.Wkb
}

func PseudoArea(val string, elem *element.OSMElem, match Match) interface{} {
	area := elem.Geom.Geom.Area()
	if area == 0.0 {
		return nil
	}
	return float32(area)
}

var wayRanks map[string]int

func init() {
	wayRanks = map[string]int{
		"minor":          3,
		"road":           3,
		"unclassified":   3,
		"residential":    3,
		"tertiary_link":  3,
		"tertiary":       4,
		"secondary_link": 3,
		"secondary":      5,
		"primary_link":   3,
		"primary":        6,
		"trunk_link":     3,
		"trunk":          8,
		"motorway_link":  3,
		"motorway":       9,
	}
}

func WayZOrder(val string, elem *element.OSMElem, match Match) interface{} {
	var z int32
	layer, _ := strconv.ParseInt(elem.Tags["layer"], 10, 64)
	z += int32(layer) * 10

	rank := wayRanks[match.Value]

	if rank == 0 {
		if _, ok := elem.Tags["railway"]; ok {
			rank = 7
		}
	}
	z += int32(rank)

	tunnel := elem.Tags["tunnel"]
	if tunnel == "true" || tunnel == "yes" || tunnel == "1" {
		z -= 10
	}
	bridge := elem.Tags["bridge"]
	if bridge == "true" || bridge == "yes" || bridge == "1" {
		z += 10
	}

	return z
}

func MakeZOrder(fieldName string, fieldType FieldType, field Field) (MakeValue, error) {
	_rankList, ok := field.Args["ranks"]
	if !ok {
		return nil, errors.New("missing ranks in args for zorder")
	}

	rankList, ok := _rankList.([]interface{})
	if !ok {
		return nil, errors.New("ranks in args for zorder not a list")
	}

	ranks := make(map[string]int)
	for i, rank := range rankList {
		rankName, ok := rank.(string)
		if !ok {
			return nil, errors.New("rank in ranks not a string")
		}

		ranks[rankName] = len(rankList) - i
	}
	zOrder := func(val string, elem *element.OSMElem, match Match) interface{} {
		if r, ok := ranks[match.Value]; ok {
			return r
		}
		return nil
	}

	return zOrder, nil
}

func MakeSuffixReplace(fieldName string, fieldType FieldType, field Field) (MakeValue, error) {
	_changes, ok := field.Args["suffixes"]
	if !ok {
		return nil, errors.New("missing suffixes in args for string_suffixreplace")
	}
	fmt.Printf("%#v\n", _changes)

	changes, ok := _changes.(map[string]string)
	if !ok {
		return nil, errors.New("suffixes in args for string_suffixreplace not a list")
	}

	var suffixes []string
	for k, _ := range changes {
		suffixes = append(suffixes, k)
	}
	reStr := `(` + strings.Join(suffixes, "|") + `)\b`
	re := regexp.MustCompile(reStr)

	replFunc := func(match string) string {
		return changes[match]
	}

	suffixReplace := func(val string, elem *element.OSMElem, match Match) interface{} {
		if val != "" {
			return re.ReplaceAllStringFunc(val, replFunc)
		}
		return val
	}

	return suffixReplace, nil
}
