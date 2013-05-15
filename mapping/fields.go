package mapping

import (
	"goposm/element"
	"log"
	"strconv"
)

type FieldSpec struct {
	Name      string
	Type      string
	ValueFunc func(string, *element.OSMElem, Match) interface{}
}

func (f *FieldSpec) Value(elem *element.OSMElem, match Match) interface{} {
	if f.ValueFunc != nil {
		return f.ValueFunc(elem.Tags[f.Name], elem, match)
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

func (t *Table) TableFields() *TableFields {
	result := TableFields{}

	for _, mappingField := range t.Fields {
		field := FieldSpec{}
		field.Name = mappingField.Key

		switch mappingField.Type {
		case "id":
			field.ValueFunc = Id
		case "string":
			field.ValueFunc = String
		case "direction":
			field.ValueFunc = Direction
		case "bool":
			field.ValueFunc = Bool
		case "integer":
			field.ValueFunc = Integer
		case "wayzorder":
			field.ValueFunc = WayZOrder
		case "mapping_key":
			field.ValueFunc = Key
		case "mapping_value":
			field.ValueFunc = Value
		case "geometry":
			field.ValueFunc = Geometry
		case "pseudoarea":
			field.ValueFunc = PseudoArea
		default:
			log.Println("unhandled type:", mappingField.Type)
		}
		result.fields = append(result.fields, field)
	}
	return &result
}

func Bool(val string, elem *element.OSMElem, match Match) interface{} {
	if val == "" || val == "0" || val == "false" || val == "no" {
		return false
	}
	return true
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
	return area
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
