//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package n1fty

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/timestamp"

	"github.com/couchbase/cbft"

	"github.com/blevesearch/bleve/mapping"

	"github.com/couchbase/n1fty/flex"
	"github.com/couchbase/n1fty/util"
)

var FlexPrefix = "-flex*"

var FlexLevel = 0 // Flex indexing is enabled when FlexLevel > 1.

func init() {
	v := os.Getenv("CB_N1FTY_FLEX")
	if v != "" {
		i, err := strconv.Atoi(v)
		if err == nil {
			FlexLevel = i
		}
	}
}

// addFlexIndexes examines the given map and adds additional FlexIndex
// entries for the FTSIndex instances that are flex indexing
// compatible.
func (i *FTSIndexer) addFlexIndexes(imap map[string]datastore.Index) (
	map[string]datastore.Index, error) {
	if FlexLevel <= 0 {
		return imap, nil
	}

	if util.Debug > 0 {
		fmt.Printf("FTSIndexer.addFlexIndexes\n")
	}

	for id, index := range imap {
		if util.Debug > 0 {
			fmt.Printf(" checking index: %s\n", id)
		}

		ftsIndex, ok := index.(*FTSIndex)
		if !ok ||
			ftsIndex == nil ||
			ftsIndex.indexDef == nil ||
			ftsIndex.indexDef.Type != "fulltext-index" ||
			ftsIndex.indexDef.Params == "" {
			if util.Debug > 0 {
				fmt.Printf("  skipping index, id: %s\n", id)
			}
			continue
		}

		bp := cbft.NewBleveParams()

		err := json.Unmarshal([]byte(ftsIndex.indexDef.Params), bp)
		if err != nil {
			if util.Debug > 0 {
				fmt.Printf("  skipping index: %s, can't unmarshal, err: %v\n",
					ftsIndex.indexDef.Name, err)
			}
			continue
		}

		if bp.DocConfig.Mode != "type_field" {
			if util.Debug > 0 {
				fmt.Printf("  skipping index: %s, wrong DocConfig.Mode: %s\n",
					ftsIndex.indexDef.Name, bp.DocConfig.Mode)
			}
			continue // TODO: Handle other DocConfig.Mode's one day.
		}

		im, ok := bp.Mapping.(*mapping.IndexMappingImpl)
		if !ok {
			if util.Debug > 0 {
				fmt.Printf("  skipping index: %s, not mapping.IndexMappingImpl\n",
					ftsIndex.indexDef.Name)
			}
			continue
		}

		condFlexIndexes, err := flex.BleveToCondFlexIndexes(im)
		if err != nil || len(condFlexIndexes) <= 0 {
			if util.Debug > 0 {
				fmt.Printf("  skipping index: %s, condFlexIndexes empty, err: %v\n",
					ftsIndex.indexDef.Name, err)
			}
			continue
		}

		flexIndex := &FlexIndex{
			ftsIndex:        ftsIndex,
			id:              FlexPrefix + ftsIndex.Id(),
			name:            FlexPrefix + ftsIndex.Name(),
			condFlexIndexes: condFlexIndexes,
		}

		imap[flexIndex.id] = flexIndex

		if util.Debug > 0 {
			fmt.Printf("  added flex-index: %s, %s\n", flexIndex.id, flexIndex.name)
		}
	}

	if util.Debug > 0 {
		fmt.Printf("FTSIndexer.addFlexIndexes, done\n\n")
	}

	return imap, nil
}

// -----------------------------------------------------------------

// A FlexIndex is implemented as a wrapper around a FTSIndex and adds
// support for flex indexing behavior.
type FlexIndex struct {
	ftsIndex *FTSIndex

	id   string
	name string

	condFlexIndexes flex.CondFlexIndexes
}

func (i *FlexIndex) KeyspaceId() string {
	return i.ftsIndex.KeyspaceId()
}

func (i *FlexIndex) Id() string {
	return i.id
}

func (i *FlexIndex) Name() string {
	return i.name
}

func (i *FlexIndex) Type() datastore.IndexType {
	return i.ftsIndex.Type()
}

func (i *FlexIndex) Indexer() datastore.Indexer {
	return i.ftsIndex.Indexer()
}

func (i *FlexIndex) SeekKey() expression.Expressions {
	return nil
}

func (i *FlexIndex) RangeKey() expression.Expressions {
	return nil
}

func (i *FlexIndex) Condition() expression.Expression {
	return nil
}

func (i *FlexIndex) IsPrimary() bool {
	return false
}

func (i *FlexIndex) State() (datastore.IndexState, string, errors.Error) {
	return datastore.ONLINE, "", nil
}

func (i *FlexIndex) Statistics(requestId string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return nil, util.N1QLError(nil, "not supported") // TODO.
}

func (i *FlexIndex) Drop(requestId string) errors.Error {
	return util.N1QLError(nil, "not supported")
}

func (i *FlexIndex) Scan(requestId string,
	span *datastore.Span, distinct bool, limit int64,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	conn.Error(util.N1QLError(nil, "not supported"))
}

func (i *FlexIndex) Sargable(field string,
	query, options expression.Expression, opaque interface{}) (
	int, int64, bool, interface{}, errors.Error) {
	return 0, 0, false, opaque, nil
}

func (i *FlexIndex) Search(requestId string,
	searchInfo *datastore.FTSSearchInfo,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	if util.Debug > 0 {
		fmt.Printf("flexIndex.Search: %s / %s,\n  requestId: %s, searchInfo: %+v, cons: %+v\n",
			i.id, i.name, requestId, searchInfo, cons)
	}

	i.ftsIndex.Search(requestId, searchInfo, cons, vector, conn)
}

func (i *FlexIndex) Pageable(order []string, offset, limit int64,
	query, options expression.Expression) bool {
	if util.Debug > 0 {
		fmt.Printf("flexIndex.Pageable: %s / %s,\n  order, %+v, offset: %d, limit: %d\n",
			i.id, i.name, order, offset, limit)
	}

	return false // TODO.
}

// -----------------------------------------------------------------

func (i *FlexIndex) SargableFlex(nodeAlias string, bindings expression.Bindings,
	where expression.Expression, opaque interface{}) (
	sargLength int, exact bool, searchQuery, searchOptions map[string]interface{},
	opaqueOut interface{}, err errors.Error) {
	if util.Debug > 0 {
		fmt.Printf("flexIndex.SargableFlex: %s / %s,\n  nodeAlias: %s\n",
			i.id, i.name, nodeAlias)
		fmt.Printf("  where: %v\n", where)
		for _, b := range bindings {
			fmt.Printf("  binding: %+v = %+v\n", b, b.Expression())
		}
	}

	identifiers := flex.Identifiers{flex.Identifier{Name: nodeAlias}}

	var ok bool
	identifiers, ok = identifiers.Push(bindings, -1)
	if !ok {
		return 0, false, nil, nil, opaque, nil
	}

	if util.Debug > 0 {
		fmt.Printf("  identifiers: %+v\n", identifiers)
	}

	fieldTracks, needsFiltering, flexBuild, err0 := i.condFlexIndexes.Sargable(
		identifiers, where, nil)
	if err0 != nil {
		if util.Debug > 0 {
			fmt.Printf("   CondFlexIndexes.Sargable err0: %v\n", err0)
		}

		return 0, false, nil, nil, opaque, util.N1QLError(err0, "")
	}

	if len(fieldTracks) <= 0 {
		if util.Debug > 0 {
			fmt.Printf("   CondFlexIndexes.Sargable: NOT-SARGABLE, len(fieldTracks) <= 0\n")
			j, _ := json.Marshal(i.condFlexIndexes)
			fmt.Printf("    len(CondFlexIndexes): %d, %s\n", len(i.condFlexIndexes), j)
		}

		return 0, false, nil, nil, opaque, nil
	}

	bleveQuery, err1 := flex.FlexBuildToBleveQuery(flexBuild, nil)
	if err1 != nil {
		if util.Debug > 0 {
			fmt.Printf("   flex.FlexBuildToBleveQuery err1: %v\n", err1)
		}

		return 0, false, nil, nil, opaque, util.N1QLError(err1, "")
	}

	searchRequest := map[string]interface{}{
		"query": bleveQuery,
		"score": "none",
	}

	searchOptions = map[string]interface{}{
		"index": i.ftsIndex.Name(),
	}

	if util.Debug > 0 {
		searchRequestJ, _ := json.Marshal(searchRequest)
		searchOptionsJ, _ := json.Marshal(searchOptions)

		fmt.Printf("  searchRequest: %s\n", searchRequestJ)
		fmt.Printf("  searchOptions: %s\n", searchOptionsJ)
		fmt.Printf("  fieldTracks: %v, exact: %v\n", fieldTracks, !needsFiltering)
	}

	return len(fieldTracks), !needsFiltering, searchRequest, searchOptions, opaque, nil
}
