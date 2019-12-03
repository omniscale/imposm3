package mapping

import (
	"errors"
	"fmt"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/mapping/config"
)

func MakeCategorizeInt(fieldName string, fieldType ColumnType, field config.Column) (MakeValue, error) {
	_values, ok := field.Args["values"]
	if !ok {
		return nil, errors.New("missing 'values' in 'args' for categorize_int")
	}

	values, ok := _values.(map[interface{}]interface{})
	if !ok {
		return nil, errors.New("'values' in 'args' for categorize_int not a dictionary")
	}

	valuesCategory := make(map[string]int)
	for value, category := range values {
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("category in values not an string key but %t", value)
		}
		c, ok := category.(int)
		if !ok {
			return nil, fmt.Errorf("category in values not an int but %t", category)
		}
		valuesCategory[v] = int(c)
	}

	_defaultCategory, ok := field.Args["default"]
	if !ok {
		return nil, errors.New("missing 'default' in categorize_int")
	}

	defaultCategoryF, ok := _defaultCategory.(int)
	if !ok {
		return nil, fmt.Errorf("'default' in 'args' for categorize_int not an int but %t", _defaultCategory)
	}
	defaultCategory := int(defaultCategoryF)

	makeValue := func(val string, elem *osm.Element, geom *geom.Geometry, m Match) interface{} {
		if val != "" {
			if cat, ok := valuesCategory[val]; ok {
				return cat
			}
		}
		for _, k := range field.Keys {
			v, ok := elem.Tags[string(k)]
			if !ok {
				continue
			}
			if cat, ok := valuesCategory[v]; ok {
				return cat
			}
		}

		return defaultCategory
	}

	return makeValue, nil
}
