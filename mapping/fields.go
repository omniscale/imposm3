package mapping

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/logging"
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
		"mapping_key":          {"mapping_key", "string", KeyName, nil},
		"mapping_value":        {"mapping_value", "string", ValueName, nil},
		"geometry":             {"geometry", "geometry", Geometry, nil},
		"validated_geometry":   {"validated_geometry", "validated_geometry", Geometry, nil},
		"hstore_tags":          {"hstore_tags", "hstore_string", HstoreString, nil},
		"wayzorder":            {"wayzorder", "int32", WayZOrder, nil},
		"pseudoarea":           {"pseudoarea", "float32", PseudoArea, nil},
		"zorder":               {"zorder", "int32", nil, MakeZOrder},
		"enumerate":            {"enumerate", "int32", nil, MakeEnumerate},
		"string_suffixreplace": {"string_suffixreplace", "string", nil, MakeSuffixReplace},
	}
}

type MakeValue func(string, *element.OSMElem, *geom.Geometry, Match) interface{}

type MakeMakeValue func(string, FieldType, Field) (MakeValue, error)

type Key string
type Value string

type FieldSpec struct {
	Key  Key
	Type FieldType
}

func (f *FieldSpec) Value(elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	if f.Type.Func != nil {
		return f.Type.Func(elem.Tags[string(f.Key)], elem, geom, match)
	}
	return nil
}

type TableFields struct {
	fields []FieldSpec
}

func (t *TableFields) MakeRow(elem *element.OSMElem, geom *geom.Geometry, match Match) []interface{} {
	var row []interface{}
	for _, field := range t.fields {
		row = append(row, field.Value(elem, geom, match))
	}
	return row
}

func (field *Field) FieldType() *FieldType {
	if fieldType, ok := AvailableFieldTypes[field.Type]; ok {
		if fieldType.MakeFunc != nil {
			makeValue, err := fieldType.MakeFunc(field.Name, fieldType, *field)
			if err != nil {
				log.Print(err)
				return nil
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
		field.Key = mappingField.Key

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

func Bool(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	if val == "" || val == "0" || val == "false" || val == "no" {
		return false
	}
	return true
}

func BoolInt(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	if val == "" || val == "0" || val == "false" || val == "no" {
		return 0
	}
	return 1
}

func String(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	return val
}

func Integer(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	v, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return nil
	}
	return v
}

func Id(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	return elem.Id
}

func KeyName(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	return match.Key
}

func ValueName(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	return match.Value
}

func Direction(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	if val == "1" || val == "yes" || val == "true" {
		return 1
	} else if val == "-1" {
		return -1
	} else {
		return 0
	}
}

func Geometry(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	return string(geom.Wkb)
}

func PseudoArea(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	area := geom.Geom.Area()
	if area == 0.0 {
		return nil
	}
	return float32(area)
}

var hstoreReplacer = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func HstoreString(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
	tags := make([]string, 0, len(elem.Tags))
	for k, v := range elem.Tags {
		tags = append(tags, `"`+hstoreReplacer.Replace(k)+`"=>"`+hstoreReplacer.Replace(v)+`"`)
	}
	return strings.Join(tags, ", ")
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

func WayZOrder(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
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
	log.Print("warn: zorder type is deprecated and will be removed. See enumerate type.")
	_rankList, ok := field.Args["ranks"]
	if !ok {
		return nil, errors.New("missing ranks in args for zorder")
	}

	rankList, ok := _rankList.([]interface{})
	if !ok {
		return nil, errors.New("ranks in args for zorder not a list")
	}

	var key string
	_key, ok := field.Args["key"]
	if ok {
		key, ok = _key.(string)
		if !ok {
			return nil, errors.New("key in args for zorder not a string")
		}
	}

	ranks := make(map[string]int)
	for i, rank := range rankList {
		rankName, ok := rank.(string)
		if !ok {
			return nil, errors.New("rank in ranks not a string")
		}

		ranks[rankName] = len(rankList) - i
	}

	zOrder := func(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
		if key != "" {
			if r, ok := ranks[elem.Tags[key]]; ok {
				return r
			}
			return 0
		}
		if r, ok := ranks[match.Value]; ok {
			return r
		}
		return 0
	}

	return zOrder, nil
}

func MakeEnumerate(fieldName string, fieldType FieldType, field Field) (MakeValue, error) {
	_valuesList, ok := field.Args["values"]
	if !ok {
		return nil, errors.New("missing values in args for enumerate")
	}

	valuesList, ok := _valuesList.([]interface{})
	if !ok {
		return nil, errors.New("values in args for enumerate not a list")
	}

	values := make(map[string]int)
	for i, value := range valuesList {
		valueName, ok := value.(string)
		if !ok {
			return nil, errors.New("value in values not a string")
		}

		values[valueName] = i + 1
	}
	enumerate := func(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
		if field.Key != "" {
			if r, ok := values[val]; ok {
				return r
			}
			return 0
		}
		if r, ok := values[match.Value]; ok {
			return r
		}
		return 0
	}

	return enumerate, nil
}

func MakeSuffixReplace(fieldName string, fieldType FieldType, field Field) (MakeValue, error) {
	_changes, ok := field.Args["suffixes"]
	if !ok {
		return nil, errors.New("missing suffixes in args for string_suffixreplace")
	}

	changes, ok := _changes.(map[string]interface{})
	if !ok {
		return nil, errors.New("suffixes in args for string_suffixreplace not a dict")
	}

	var suffixes []string
	for k, _ := range changes {
		suffixes = append(suffixes, k)
	}
	reStr := `(` + strings.Join(suffixes, "|") + `)\b`
	re := regexp.MustCompile(reStr)

	replFunc := func(match string) string {
		return changes[match].(string)
	}

	suffixReplace := func(val string, elem *element.OSMElem, geom *geom.Geometry, match Match) interface{} {
		if val != "" {
			return re.ReplaceAllStringFunc(val, replFunc)
		}
		return val
	}

	return suffixReplace, nil
}

func asHex(b []byte) string {
	digits := "0123456789ABCDEF"
	buf := make([]byte, 0, len(b)*2)
	n := len(b)

	for i := 0; i < n; i++ {
		c := b[i]
		buf = append(buf, digits[c>>4], digits[c&0xF])
	}
	return string(buf)
}
