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

package n1fty

import (
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/search/query"
)

func updateFieldsInQuery(q query.Query, field string) error {
	switch q.(type) {
	case *query.BooleanQuery:
		updateFieldsInQuery(q.(*query.BooleanQuery).Must, field)
		updateFieldsInQuery(q.(*query.BooleanQuery).Should, field)
		updateFieldsInQuery(q.(*query.BooleanQuery).MustNot, field)
	case *query.ConjunctionQuery:
		cq := q.(*query.ConjunctionQuery)
		for i := 0; i < len(cq.Conjuncts); i++ {
			updateFieldsInQuery(cq.Conjuncts[i], field)
		}
	case *query.DisjunctionQuery:
		dq := q.(*query.DisjunctionQuery)
		for i := 0; i < len(dq.Disjuncts); i++ {
			updateFieldsInQuery(dq.Disjuncts[i], field)
		}
	default:
		if fq, ok := q.(query.FieldableQuery); ok {
			if fq.Field() == "" {
				fq.SetField(field)
			}
		}
	}

	return nil
}

type Options struct {
	Type         string  `json:"type"`
	Analyzer     string  `json:"analyzer"`
	Boost        float64 `json:"boost"`
	Fuzziness    int     `json:"fuzziness"`
	PrefixLength int     `json:"prefix_length"`
	Operator     string  `json:"operator"`
}

// -----------------------------------------------------------------------------

func PrepQuery(field, input, options string) (query.Query, error) {
	qBytes, err := PrepQueryBytes(field, input, options)
	if err != nil {
		return nil, fmt.Errorf("PrepQuery err: %v", err)
	}

	return query.ParseQuery(qBytes)
}

func PrepQueryBytes(field, input, options string) ([]byte, error) {
	opt := Options{}
	if options != "" {
		err := json.Unmarshal([]byte(options), &opt)
		if err != nil {
			return nil, fmt.Errorf("err: %v", err)
		}
	}

	switch opt.Type {
	case "query_string++":
		fallthrough
	case "search":
		fallthrough
	case "":
		fallthrough
	case "query_string":
		fallthrough
	case "query":
		qsq := query.NewQueryStringQuery(input)
		q, err := qsq.Parse()
		if err != nil {
			return nil, fmt.Errorf("PrepQueryBytes Parse, err: %v", err)
		}

		if field != "" && field != "_all" {
			err = updateFieldsInQuery(q, field)
			if err != nil {
				return nil, fmt.Errorf("PrepQueryBytes updateFieldsInQuery,"+
					" err: %v", err)
			}
		}

		return json.Marshal(q)
	case "bool":
		fallthrough
	case "match_phrase":
		fallthrough
	case "match":
		fallthrough
	case "prefix":
		fallthrough
	case "regexp":
		fallthrough
	case "wildcard":
		fallthrough
	case "terms":
		fallthrough
	case "term":
		output := map[string]interface{}{}
		if field != "" && field != "_all" {
			output["field"] = field
		}
		output[opt.Type] = input
		if opt.Analyzer != "" {
			output["analyzer"] = opt.Analyzer
		}
		output["boost"] = opt.Boost
		if opt.Fuzziness > 0 {
			output["fuzziness"] = opt.Fuzziness
		}
		if opt.PrefixLength > 0 {
			output["prefix_length"] = opt.PrefixLength
		}
		if opt.Operator != "" {
			output["operator"] = opt.Operator
		}
		return json.Marshal(output)
	default:
		return nil, fmt.Errorf("PrepQueryBytes not supported: %v", opt.Type)
	}
}
