package config

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

type Mapping struct {
	Tables            Tables            `yaml:"tables"`
	GeneralizedTables GeneralizedTables `yaml:"generalized_tables"`
	Tags              Tags              `yaml:"tags"`
	Areas             Areas             `yaml:"areas"`
	// SingleIdSpace mangles the overlapping node/way/relation IDs
	// to be unique (nodes positive, ways negative, relations negative -1e17)
	SingleIdSpace bool `yaml:"use_single_id_space"`
}

type Column struct {
	Name       string                 `yaml:"name"`
	Key        Key                    `yaml:"key"`
	Keys       []Key                  `yaml:"keys"`
	Type       string                 `yaml:"type"`
	Args       map[string]interface{} `yaml:"args"`
	FromMember bool                   `yaml:"from_member"`
}

type Tables map[string]*Table
type Table struct {
	Name          string
	Type          string                `yaml:"type"`
	Mapping       KeyValues             `yaml:"mapping"`
	Mappings      map[string]SubMapping `yaml:"mappings"`
	TypeMappings  TypeMappings          `yaml:"type_mappings"`
	Columns       []*Column             `yaml:"columns"`
	OldFields     []*Column             `yaml:"fields"`
	Filters       *Filters              `yaml:"filters"`
	RelationTypes []string              `yaml:"relation_types"`
}

type GeneralizedTables map[string]*GeneralizedTable
type GeneralizedTable struct {
	Name            string
	SourceTableName string  `yaml:"source"`
	Tolerance       float64 `yaml:"tolerance"`
	SqlFilter       string  `yaml:"sql_filter"`
}

type Filters struct {
	ExcludeTags   *[][]string    `yaml:"exclude_tags"`
	Reject        KeyValues      `yaml:"reject"`
	Require       KeyValues      `yaml:"require"`
	RejectRegexp  KeyRegexpValue `yaml:"reject_regexp"`
	RequireRegexp KeyRegexpValue `yaml:"require_regexp"`
}

type Areas struct {
	AreaTags   []Key `yaml:"area_tags"`
	LinearTags []Key `yaml:"linear_tags"`
}

type Tags struct {
	LoadAll bool  `yaml:"load_all"`
	Exclude []Key `yaml:"exclude"`
	Include []Key `yaml:"include"`
}

type Key string
type Value string

type OrderedValue struct {
	Value
	Order int
}

type KeyValues map[Key][]OrderedValue
type KeyRegexpValue map[Key]string

func (kv *KeyValues) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if *kv == nil {
		*kv = make(map[Key][]OrderedValue)
	}
	slice := yaml.MapSlice{}
	err := unmarshal(&slice)
	if err != nil {
		return err
	}
	order := 0
	for _, item := range slice {
		k, ok := item.Key.(string)
		if !ok {
			return fmt.Errorf("mapping key '%s' not a string", k)
		}
		values, ok := item.Value.([]interface{})
		if !ok {
			return fmt.Errorf("mapping key '%s' not a string", k)
		}
		for _, v := range values {
			if v, ok := v.(string); ok {
				(*kv)[Key(k)] = append((*kv)[Key(k)], OrderedValue{Value: Value(v), Order: order})
			} else {
				return fmt.Errorf("mapping value '%s' not a string", v)
			}
			order += 1
		}
	}
	return nil
}

type SubMapping struct {
	Mapping KeyValues
}

type TypeMappings struct {
	Points      KeyValues `yaml:"points"`
	LineStrings KeyValues `yaml:"linestrings"`
	Polygons    KeyValues `yaml:"polygons"`
}
