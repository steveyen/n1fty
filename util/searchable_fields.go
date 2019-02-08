// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

package util

import (
	"encoding/json"

	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/logging"
)

func SearchableFieldsForIndexDef(indexDef *cbgt.IndexDef) (
	map[string][]string, bool) {
	bp := cbft.NewBleveParams()
	err := json.Unmarshal([]byte(indexDef.Params), bp)
	if err != nil {
		logging.Infof("n1fty: convertIndexDefs skip indexDef: %+v,"+
			" json unmarshal indexDef.Params, err: %v\n", indexDef, err)
		return nil, false
	}

	if bp.DocConfig.Mode != "type_field" {
		logging.Infof("n1fty: convertIndexDefs skip indexDef: %+v,"+
			" wrong DocConfig.Mode\n", indexDef)
		return nil, false
	}

	typeField := bp.DocConfig.TypeField
	if typeField == "" {
		logging.Infof("n1fty: convertIndexDefs skip indexDef: %+v,"+
			" wrong DocConfig.TypeField\n", typeField)
		return nil, false
	}

	bm, ok := bp.Mapping.(*mapping.IndexMappingImpl)
	if !ok {
		logging.Infof("n1fty: convertIndexDefs skip indexDef: %+v, "+
			" not IndexMappingImpl\n", *indexDef)
		return nil, false
	}

	searchableFieldsMap := map[string][]string{}

	for typeName, typeMapping := range bm.TypeMapping {
		if typeMapping.Enabled {
			if typeMapping.Dynamic {
				// everything under document type is indexed
				searchableFieldsMap[typeName] = []string{"_all"}
			} else {
				searchableFieldsMap[typeName] = fetchSearchableFields("", typeMapping)
			}
		}
	}

	defaultMappingDynamic := false
	if bm.DefaultMapping != nil && bm.DefaultMapping.Enabled {
		if bm.DefaultMapping.Dynamic {
			searchableFieldsMap["default"] = []string{"_all"}
			defaultMappingDynamic = true
		} else {
			rv := fetchSearchableFields("", bm.DefaultMapping)
			searchableFieldsMap["default"] = rv
		}
	}

	return searchableFieldsMap, defaultMappingDynamic
}

func fetchSearchableFields(path string, typeMapping *mapping.DocumentMapping) []string {
	rv := []string{}

	if len(typeMapping.Fields) == 0 && len(typeMapping.Properties) == 0 &&
		typeMapping.Enabled && typeMapping.Dynamic {
		rv = append(rv, "_all")
		return rv
	}

	for _, field := range typeMapping.Fields {
		if field.Index {
			if len(path) == 0 {
				rv = append(rv, field.Name)
			} else {
				rv = append(rv, path+"."+field.Name)
			}
		}
	}

	for childMappingName, childMapping := range typeMapping.Properties {
		newPath := path
		if len(childMapping.Fields) == 0 {
			if len(path) == 0 {
				newPath = childMappingName
			} else {
				newPath += "." + childMappingName
			}
		}
		if typeMapping.Enabled {
			if typeMapping.Dynamic {
				rv = append(rv, "_all")
			} else {
				extra := fetchSearchableFields(newPath, childMapping)
				rv = append(rv, extra...)
			}
		}
	}

	return rv
}

// -----------------------------------------------------------------------------

func FetchFieldsToSearch(field, q, options string) ([]string, error) {
	que, err := BuildQuery(field, q, options)
	if err != nil {
		return nil, err
	}

	fields := []string{}
	var walk func(que query.Query)

	walk = func(que query.Query) {
		switch qq := que.(type) {
		case *query.BooleanQuery:
			walk(qq.Must)
			walk(qq.MustNot)
			walk(qq.Should)
		case *query.ConjunctionQuery:
			for _, childQ := range qq.Conjuncts {
				walk(childQ)
			}
		case *query.DisjunctionQuery:
			for _, childQ := range qq.Disjuncts {
				walk(childQ)
			}
		default:
			if fq, ok := que.(query.FieldableQuery); ok {
				fields = append(fields, fq.Field())
			}
		}
	}

	walk(que)
	return fields, nil
}