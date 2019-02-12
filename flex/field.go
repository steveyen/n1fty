//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package flex

import (
	"github.com/couchbase/query/expression"
)

type Identifier struct {
	Name      string
	Expansion []string // May be empty when no expansion.
}

type Identifiers []Identifier // A left-pushed stack of identifiers.

// ---------------------------------------------------------------

func (identifiers Identifiers) PushBindings(bs expression.Bindings, max int) (
	Identifiers, bool) {
	rootIdentifier := identifiers[len(identifiers)-1].Name

	for i, b := range bs {
		if max > 0 && max <= i {
			return nil, false
		}

		suffix, ok := ExpressionFieldPathSuffix(identifiers, b.Expression(), nil, nil)
		if ok {
			identifiers = append(Identifiers{Identifier{
				Name:      b.Variable(),
				Expansion: append([]string{rootIdentifier}, suffix...),
			}}, identifiers...)
		}
	}

	return identifiers, true
}

// ReverseExpand finds the identifierName in the identifiers and on
// success will reverse-append the expansions onto the out slice.
// For example, if identifiers is...
//   [ Identifier{"w",   ["v", "address", "city"]},
//     Identifier{"v",   ["emp", "locations"]},
//     Identifier{"emp", nil} ],
// and identifierName is "w", and out is [], then returned will be...
//   ["city", "address", "locations"], true
func (identifiers Identifiers) ReverseExpand(
	identifierName string, out []string) ([]string, bool) {
	for _, identifier := range identifiers {
		if identifier.Name == identifierName {
			if len(identifier.Expansion) <= 0 {
				return out, true
			}

			for i := len(identifier.Expansion) - 1; i > 0; i-- {
				out = append(out, identifier.Expansion[i])
			}

			identifierName = identifier.Expansion[0]
		}
	}

	return out[:0], false
}

// ---------------------------------------------------------------

// A field name is part of a field path, like "name", or "city".
//
// A field path is an absolute, fully qualified []string, like...
//   []string{"offices", "addr", "city"}.
//
// A field track is a field path joined by "." separator, like...
//   "offices.addr.city".

type FieldTrack string
type FieldTracks map[FieldTrack]int

// ---------------------------------------------------------------

type FieldInfo struct {
	FieldPath []string // Ex: ["name"], ["addr", "city"], [].
	FieldType string   // Ex: "string", "number".

	// TODO: One day when we support multiple type mappings...
	// AllowedDocTypes    []string // Ex: { "type": "beer", ... }.
	// DisallowedDocTypes []string
}

type FieldInfos []*FieldInfo

// ---------------------------------------------------------------

// Given an expression.Field, finds the first FieldInfo that has a
// matching field-path prefix.  The suffixOut enables slice reuse.
func (fieldInfos FieldInfos) Find(identifiers Identifiers,
	f expression.Expression, suffixOut []string) (*FieldInfo, []string) {
	var ok bool

	for _, fieldInfo := range fieldInfos {
		suffixOut, ok = ExpressionFieldPathSuffix(identifiers, f,
			fieldInfo.FieldPath, suffixOut[:0])
		if ok {
			return fieldInfo, suffixOut
		}
	}

	return nil, suffixOut[:0]
}

// ---------------------------------------------------------------

// Recursively checks if expr uses any of field in the fieldInfos,
// where the fields all resolve to the same root identifier scope.
//
// Example: if fieldInfos has 'price' and 'cost', then...
// - `rootIdentifier`.`price` > 100 ==> true.
// - `unknownIdentifier`.`cost` > 100 ==> false.

func CheckFieldsUsed(fieldInfos FieldInfos, identifiers Identifiers,
	expr expression.Expression) (found bool, err error) {
	var fieldInfo *FieldInfo
	var suffix []string

	m := &expression.MapperBase{}

	m.SetMapper(m)

	m.SetMapFunc(func(e expression.Expression) (expression.Expression, error) {
		if fieldInfo == nil {
			f, ok := e.(*expression.Field)
			if ok {
				fieldInfo, suffix = fieldInfos.Find(identifiers, f, suffix[:0])
			}

			return e, e.MapChildren(m)
		}

		return e, nil // When found, avoid additional recursion.
	})

	_, err = m.Map(expr)

	return fieldInfo != nil, err
}

// ---------------------------------------------------------------

// Checks whether an expr, such as
// `emp`.`locations`.`work`.`address`.`city`, is a nested fields
// reference against the identifiers (e.g., "emp"), and also has a
// given prefix (e.g., ["locations", "work"]), and if so returns the
// remaining field path suffix (e.g., ["address", "city"]).
// The prefix may be [].  The returned suffix may be [].
// The suffixOut allows for slice reuse.
func ExpressionFieldPathSuffix(identifiers Identifiers,
	expr expression.Expression, prefix []string,
	suffixOut []string) ([]string, bool) {
	suffixReverse := suffixOut[:0] // Reuse slice.

	var visit func(e expression.Expression) bool // Declare for recursion.

	visitField := func(f *expression.Field) bool {
		suffixReverse = append(suffixReverse, f.Second().Alias())

		return visit(f.First())
	}

	visitIdentifier := func(e expression.Expression) bool {
		i, ok := e.(*expression.Identifier)
		if ok {
			suffixReverse, ok = identifiers.ReverseExpand(
				i.Identifier(), suffixReverse)

			return ok
		}

		return false
	}

	visit = func(e expression.Expression) bool {
		if f, ok := e.(*expression.Field); ok {
			return visitField(f)
		}

		return visitIdentifier(e)
	}

	if !visit(expr) {
		return suffixOut[:0], false
	}

	// Compare suffixReverse (["city", "address", "work", "locations"])
	// with the prefix (["locations", "work"]).
	if len(prefix) > len(suffixReverse) {
		return suffixOut[:0], false
	}

	for _, s := range prefix {
		if s != suffixReverse[len(suffixReverse)-1] {
			return suffixOut[:0], false
		}

		suffixReverse = suffixReverse[0 : len(suffixReverse)-1]
	}

	if len(suffixReverse) <= 0 {
		return suffixOut[:0], true
	}

	return reverseInPlace(suffixReverse), true
}

func reverseInPlace(a []string) []string {
	last := len(a) - 1
	mid := len(a) / 2

	for i := 0; i < mid; i++ {
		a[i], a[last-i] = a[last-i], a[i]
	}

	return a
}