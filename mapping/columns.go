package mapping

import (
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/log"

	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/pkg/errors"
)

var AvailableColumnTypes map[string]ColumnType

func init() {
	AvailableColumnTypes = map[string]ColumnType{
		"bool":                 {"bool", "bool", Bool, nil, nil, false},
		"boolint":              {"boolint", "int8", BoolInt, nil, nil, false},
		"id":                   {"id", "int64", ID, nil, nil, false},
		"string":               {"string", "string", String, nil, nil, false},
		"direction":            {"direction", "int8", Direction, nil, nil, false},
		"integer":              {"integer", "int32", Integer, nil, nil, false},
		"mapping_key":          {"mapping_key", "string", KeyName, nil, nil, false},
		"mapping_value":        {"mapping_value", "string", ValueName, nil, nil, false},
		"member_id":            {"member_id", "int64", nil, nil, RelationMemberID, true},
		"member_role":          {"member_role", "string", nil, nil, RelationMemberRole, true},
		"member_type":          {"member_type", "int8", nil, nil, RelationMemberType, true},
		"member_index":         {"member_index", "int32", nil, nil, RelationMemberIndex, true},
		"geometry":             {"geometry", "geometry", Geometry, nil, nil, false},
		"validated_geometry":   {"validated_geometry", "validated_geometry", Geometry, nil, nil, false},
		"hstore_tags":          {"hstore_tags", "hstore_string", nil, MakeHStoreString, nil, false},
		"jsonb_tags":          {"jsonb_tags", "jsonb_string", nil, MakeJSONBString, nil, false},
		"wayzorder":            {"wayzorder", "int32", nil, MakeWayZOrder, nil, false},
		"pseudoarea":           {"pseudoarea", "float32", nil, MakePseudoArea, nil, false},
		"area":                 {"area", "float32", Area, nil, nil, false},
		"webmerc_area":         {"webmerc_area", "float32", WebmercArea, nil, nil, false},
		"zorder":               {"zorder", "int32", nil, MakeZOrder, nil, false},
		"enumerate":            {"enumerate", "int32", nil, MakeEnumerate, nil, false},
		"string_suffixreplace": {"string_suffixreplace", "string", nil, MakeSuffixReplace, nil, false},

		"categorize_int":             {Name: "categorize_int", GoType: "int32", MakeFunc: MakeCategorizeInt},
		"geojson_intersects":         {Name: "geojson_intersects", GoType: "bool", MakeFunc: MakeIntersectsField},
		"geojson_intersects_feature": {Name: "geojson_intersects_feature", GoType: "string", MakeFunc: MakeIntersectsFeatureField},
	}
}

type MakeValue func(string, *osm.Element, *geom.Geometry, Match) interface{}
type MakeMemberValue func(*osm.Relation, *osm.Member, int, Match) interface{}

type MakeMakeValue func(string, ColumnType, config.Column) (MakeValue, error)

type Key string
type Value string

type ColumnType struct {
	Name       string
	GoType     string
	Func       MakeValue
	MakeFunc   MakeMakeValue
	MemberFunc MakeMemberValue
	FromMember bool
}

func Bool(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	if val == "" || val == "0" || val == "false" || val == "no" {
		return false
	}
	return true
}

func BoolInt(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	if val == "" || val == "0" || val == "false" || val == "no" {
		return 0
	}
	return 1
}

func String(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	return val
}

func Integer(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	v, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return nil
	}
	return v
}

func ID(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	return elem.ID
}

func KeyName(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	return match.Key
}

func ValueName(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	return match.Value
}

func RelationMemberType(rel *osm.Relation, member *osm.Member, memberIndex int, match Match) interface{} {
	return member.Type
}

func RelationMemberRole(rel *osm.Relation, member *osm.Member, memberIndex int, match Match) interface{} {
	return member.Role
}

func RelationMemberID(rel *osm.Relation, member *osm.Member, memberIndex int, match Match) interface{} {
	return member.ID
}

func RelationMemberIndex(rel *osm.Relation, member *osm.Member, memberIndex int, match Match) interface{} {
	return memberIndex
}

func Direction(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	if val == "1" || val == "yes" || val == "true" {
		return 1
	} else if val == "-1" {
		return -1
	} else {
		return 0
	}
}

func Geometry(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	return string(geom.Wkb)
}

func MakePseudoArea(columnName string, columnType ColumnType, column config.Column) (MakeValue, error) {
	log.Println("[warn] pseudoarea type is deprecated and will be removed. See area and webmerc_area type.")
	return Area, nil
}

func Area(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	if geom.Geom == nil {
		return nil
	}
	area := geom.Geom.Area()
	if area == 0.0 {
		return nil
	}
	return float32(area)
}

func WebmercArea(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	if geom.Geom == nil {
		return nil
	}
	area := geom.Geom.Area()
	if area == 0.0 {
		return nil
	}

	bounds := geom.Geom.Bounds()
	midY := bounds.MinY + (bounds.MaxY-bounds.MinY)/2

	pole := 6378137 * math.Pi // 20037508.342789244
	midLat := 2*math.Atan(math.Exp((midY/pole)*math.Pi)) - math.Pi/2

	area = area * math.Pow(math.Cos(midLat), 2)

	return float32(area)
}

var hstoreReplacer = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func MakeHStoreString(columnName string, columnType ColumnType, column config.Column) (MakeValue, error) {
	var includeAll bool
	var err error
	var include map[string]int
	if _, ok := column.Args["include"]; !ok {
		includeAll = true
	} else {
		include, err = decodeEnumArg(column, "include")
		if err != nil {
			return nil, err
		}

	}
	hstoreString := func(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
		tags := make([]string, 0, len(elem.Tags))
		for k, v := range elem.Tags {
			if includeAll || include[k] != 0 {
				tags = append(tags, `"`+hstoreReplacer.Replace(k)+`"=>"`+hstoreReplacer.Replace(v)+`"`)
			}
		}
		return strings.Join(tags, ", ")
	}
	return hstoreString, nil
}


func MakeJSONBString(columnName string, columnType ColumnType, column config.Column) (MakeValue, error) {
	var includeAll bool
	var err error
	var include map[string]int
	if _, ok := column.Args["include"]; !ok {
		includeAll = true
	} else {
		include, err = decodeEnumArg(column, "include")
		if err != nil {
			return nil, err
		}

	}

	jsonbString := func(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
		tags := make(map[string]string)
		for k, v := range elem.Tags {
			if includeAll || include[k] != 0 {
				tags[k] = v
			}
		}
		json, _ := json.Marshal(tags)
		return string(json);
	}
	return jsonbString, nil
}

func MakeWayZOrder(columnName string, columnType ColumnType, column config.Column) (MakeValue, error) {
	if _, ok := column.Args["ranks"]; !ok {
		return DefaultWayZOrder, nil
	}
	ranks, err := decodeEnumArg(column, "ranks")
	if err != nil {
		return nil, err
	}
	levelOffset := len(ranks)

	defaultRank := 0
	if val, ok := column.Args["default"].(float64); ok {
		defaultRank = int(val)
	}

	wayZOrder := func(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
		var z int
		layer, _ := strconv.ParseInt(elem.Tags["layer"], 10, 32)

		z += int(layer) * levelOffset

		rank, ok := ranks[match.Value]
		if !ok {
			z += defaultRank
		}

		z += rank

		tunnel := elem.Tags["tunnel"]
		if tunnel == "true" || tunnel == "yes" || tunnel == "1" {
			z -= levelOffset
		}
		bridge := elem.Tags["bridge"]
		if bridge == "true" || bridge == "yes" || bridge == "1" {
			z += levelOffset
		}

		if z < math.MinInt32 || z > math.MaxInt32 {
			return nil
		}
		return z
	}
	return wayZOrder, nil
}

var defaultRanks map[string]int

func init() {
	defaultRanks = map[string]int{
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

func DefaultWayZOrder(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
	var z int
	layer, _ := strconv.ParseInt(elem.Tags["layer"], 10, 64)
	z += int(layer) * 10

	rank := defaultRanks[match.Value]

	if rank == 0 {
		if _, ok := elem.Tags["railway"]; ok {
			rank = 7
		}
	}
	z += rank

	tunnel := elem.Tags["tunnel"]
	if tunnel == "true" || tunnel == "yes" || tunnel == "1" {
		z -= 10
	}
	bridge := elem.Tags["bridge"]
	if bridge == "true" || bridge == "yes" || bridge == "1" {
		z += 10
	}

	if z < math.MinInt32 || z > math.MaxInt32 {
		return nil
	}
	return z
}

func MakeZOrder(columnName string, columnType ColumnType, column config.Column) (MakeValue, error) {
	log.Println("[warn] zorder type is deprecated and will be removed. See enumerate type.")
	_rankList, ok := column.Args["ranks"]
	if !ok {
		return nil, errors.New("missing ranks in args for zorder")
	}

	rankList, ok := _rankList.([]interface{})
	if !ok {
		return nil, errors.New("ranks in args for zorder not a list")
	}

	var key string
	_key, ok := column.Args["key"]
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

	zOrder := func(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
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

func MakeEnumerate(columnName string, columnType ColumnType, column config.Column) (MakeValue, error) {
	values, err := decodeEnumArg(column, "values")
	if err != nil {
		return nil, err
	}
	enumerate := func(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
		if column.Key != "" {
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

func decodeEnumArg(column config.Column, key string) (map[string]int, error) {
	_valuesList, ok := column.Args[key]
	if !ok {
		return nil, errors.Errorf("missing '%v' in args for %s", key, column.Type)
	}

	valuesList, ok := _valuesList.([]interface{})
	if !ok {
		return nil, errors.Errorf("'%v' in args for %s not a list", key, column.Type)
	}

	values := make(map[string]int)
	for i, value := range valuesList {
		valueName, ok := value.(string)
		if !ok {
			return nil, errors.Errorf("value in '%v' not a string", key)
		}

		values[valueName] = i + 1
	}
	return values, nil
}

func MakeSuffixReplace(columnName string, columnType ColumnType, column config.Column) (MakeValue, error) {
	_changes, ok := column.Args["suffixes"]
	if !ok {
		return nil, errors.New("missing suffixes in args for string_suffixreplace")
	}

	changes, ok := _changes.(map[interface{}]interface{})
	if !ok {
		return nil, errors.New("suffixes in args for string_suffixreplace not a dict")
	}
	strChanges := make(map[string]string, len(changes))
	for k, v := range changes {
		_, kok := k.(string)
		_, vok := v.(string)
		if !kok || !vok {
			return nil, errors.New("suffixes in args for string_suffixreplace not strings")
		}
		strChanges[k.(string)] = v.(string)
	}
	var suffixes []string
	for k := range strChanges {
		suffixes = append(suffixes, k)
	}
	reStr := `(` + strings.Join(suffixes, "|") + `)\b`
	re := regexp.MustCompile(reStr)

	replFunc := func(match string) string {
		return strChanges[match]
	}

	suffixReplace := func(val string, elem *osm.Element, geom *geom.Geometry, match Match) interface{} {
		if val != "" {
			return re.ReplaceAllStringFunc(val, replFunc)
		}
		return val
	}

	return suffixReplace, nil
}
