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

package verify

import (
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/value"
)

func NewVerify(keyspace, field string, query, options value.Value) (
	datastore.Verify, errors.Error) {
	// FIXME
	return &VerifyCtx{}, nil
}

type VerifyCtx struct {
	// PlaceHolder
}

func (v *VerifyCtx) Evaluate(item value.Value) (bool, errors.Error) {
	// FIXME
	return true, nil
}