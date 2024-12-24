/**
 * Copyright 2015 The Kubernetes Authors.
 * Copyright 2024 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Copied and adapted from https://github.com/kubernetes/client-go/blob/master/util/jsonpath/jsonpath.go

package extracter

import (
	"fmt"
	"reflect"
	"strings"

	"k8s.io/client-go/third_party/forked/golang/template"
	"k8s.io/client-go/util/jsonpath"
)

type jsonPathExtracter struct {
	name       string
	parser     *parser
	beginRange int
	inRange    int
	endRange   int

	lastEndNode *jsonpath.Node

	allowMissingKeys bool
}

// NewJSONPathExtracter creates a new JSONPathExtracter with the given parser.
func NewJSONPathExtracter(parser *parser, allowMissingKeys bool) Extracter {
	return &jsonPathExtracter{
		name:       parser.Name,
		beginRange: 0,
		inRange:    0,
		endRange:   0,

		parser:           parser,
		allowMissingKeys: allowMissingKeys,
	}
}

type setFieldFunc func(val reflect.Value) error

var nopSetFieldFunc = func(_ reflect.Value) error { return nil }

func makeNopSetFieldFuncSlice(n int) []setFieldFunc {
	fns := make([]setFieldFunc, n)
	for i := 0; i < n; i++ {
		fns[i] = nopSetFieldFunc
	}
	return fns
}

// Extract outputs the field specified by JSONPath.
// The output contains not only the field value, but also its upstream structure.
//
// The data structure of the extracted field must be of type `map[string]interface{}`,
// and `struct` is not supported (an error will be returned).
func (j *jsonPathExtracter) Extract(data map[string]interface{}) (map[string]interface{}, error) {
	container := struct{ Root reflect.Value }{}
	setFn := func(val reflect.Value) error {
		container.Root = val
		return nil
	}

	_, err := j.findResults(data, setFn)
	if err != nil {
		return nil, err
	}

	if !container.Root.IsValid() {
		return nil, nil
	}

	return container.Root.Interface().(map[string]interface{}), nil
}

func (j *jsonPathExtracter) findResults(data interface{}, setFn setFieldFunc) ([][]reflect.Value, error) {
	if j.parser == nil {
		return nil, fmt.Errorf("%s is an incomplete jsonpath template", j.name)
	}

	cur := []reflect.Value{reflect.ValueOf(data)}
	curnFn := []setFieldFunc{setFn}
	nodes := j.parser.Root.Nodes
	fullResult := [][]reflect.Value{}
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		results, fn, err := j._walk(cur, node, curnFn)
		if err != nil {
			return nil, err
		}

		// encounter an end node, break the current block
		if j.endRange > 0 && j.endRange <= j.inRange {
			j.endRange--
			j.lastEndNode = &nodes[i]
			break
		}
		// encounter a range node, start a range loop
		if j.beginRange > 0 {
			j.beginRange--
			j.inRange++
			if len(results) > 0 {
				for ri, value := range results {
					j.parser.Root.Nodes = nodes[i+1:]
					nextResults, err := j.findResults(value.Interface(), fn[ri])
					if err != nil {
						return nil, err
					}
					fullResult = append(fullResult, nextResults...)
				}
			} else {
				// If the range has no results, we still need to process the nodes within the range
				// so the position will advance to the end node
				j.parser.Root.Nodes = nodes[i+1:]
				_, err := j.findResults(nil, nopSetFieldFunc)
				if err != nil {
					return nil, err
				}
			}
			j.inRange--

			// Fast forward to resume processing after the most recent end node that was encountered
			for k := i + 1; k < len(nodes); k++ {
				if &nodes[k] == j.lastEndNode {
					i = k
					break
				}
			}
			continue
		}
		fullResult = append(fullResult, results)
	}
	return fullResult, nil
}

func (j *jsonPathExtracter) _walk(value []reflect.Value, node jsonpath.Node, setFn []setFieldFunc) ([]reflect.Value, []setFieldFunc, error) {
	switch node := node.(type) {
	case *jsonpath.ListNode:
		return j._evalList(value, node, setFn)
	case *jsonpath.FieldNode:
		return j.evalField(value, node, setFn)
	case *jsonpath.ArrayNode:
		return j.evalArray(value, node, setFn)
	case *jsonpath.IdentifierNode:
		return j.evalIdentifier(value, node, setFn)
	case *jsonpath.UnionNode:
		return j._evalUnion(value, node, setFn)
	case *jsonpath.FilterNode:
		return j.evalFilter(value, node, setFn)
	default:
		return nil, nil, fmt.Errorf("Extract does not support node %v", node)
	}
}

// walk visits tree rooted at the given node in DFS order
func (j *jsonPathExtracter) walk(value []reflect.Value, node jsonpath.Node) ([]reflect.Value, error) {
	var err error
	switch node := node.(type) {
	case *jsonpath.ListNode:
		return j.evalList(value, node)
	case *jsonpath.TextNode:
		return []reflect.Value{reflect.ValueOf(node.Text)}, nil
	case *jsonpath.FieldNode:
		value, _, err = j.evalField(value, node, makeNopSetFieldFuncSlice(len(value)))
		return value, err
	case *jsonpath.ArrayNode:
		value, _, err = j.evalArray(value, node, makeNopSetFieldFuncSlice(len(value)))
		return value, err
	case *jsonpath.FilterNode:
		value, _, err = j.evalFilter(value, node, makeNopSetFieldFuncSlice(len(value)))
		return value, err
	case *jsonpath.IntNode:
		return j.evalInt(value, node)
	case *jsonpath.BoolNode:
		return j.evalBool(value, node)
	case *jsonpath.FloatNode:
		return j.evalFloat(value, node)
	case *jsonpath.WildcardNode:
		return j.evalWildcard(value)
	case *jsonpath.RecursiveNode:
		return j.evalRecursive(value)
	case *jsonpath.UnionNode:
		return j.evalUnion(value, node)
	case *jsonpath.IdentifierNode:
		value, _, err = j.evalIdentifier(value, node, makeNopSetFieldFuncSlice(len(value)))
		return value, err
	default:
		return value, fmt.Errorf("unexpected Node %v", node)
	}
}

// evalInt evaluates IntNode
func (j *jsonPathExtracter) evalInt(input []reflect.Value, node *jsonpath.IntNode) ([]reflect.Value, error) {
	result := make([]reflect.Value, len(input))
	for i := range input {
		result[i] = reflect.ValueOf(node.Value)
	}
	return result, nil
}

// evalFloat evaluates FloatNode
func (j *jsonPathExtracter) evalFloat(input []reflect.Value, node *jsonpath.FloatNode) ([]reflect.Value, error) {
	result := make([]reflect.Value, len(input))
	for i := range input {
		result[i] = reflect.ValueOf(node.Value)
	}
	return result, nil
}

// evalBool evaluates BoolNode
func (j *jsonPathExtracter) evalBool(input []reflect.Value, node *jsonpath.BoolNode) ([]reflect.Value, error) {
	result := make([]reflect.Value, len(input))
	for i := range input {
		result[i] = reflect.ValueOf(node.Value)
	}
	return result, nil
}

func (j *jsonPathExtracter) _evalList(value []reflect.Value, node *jsonpath.ListNode, setFn []setFieldFunc) ([]reflect.Value, []setFieldFunc, error) {
	var err error
	curValue := value
	curFns := setFn

	for _, node := range node.Nodes {
		curValue, curFns, err = j._walk(curValue, node, curFns)
		if err != nil {
			return curValue, curFns, err
		}
	}
	return curValue, curFns, nil
}

// evalList evaluates ListNode
func (j *jsonPathExtracter) evalList(value []reflect.Value, node *jsonpath.ListNode) ([]reflect.Value, error) {
	var err error
	curValue := value
	for _, node := range node.Nodes {
		curValue, err = j.walk(curValue, node)
		if err != nil {
			return curValue, err
		}
	}
	return curValue, nil
}

// evalIdentifier evaluates IdentifierNode
func (j *jsonPathExtracter) evalIdentifier(input []reflect.Value, node *jsonpath.IdentifierNode, setFn []setFieldFunc) ([]reflect.Value, []setFieldFunc, error) {
	results := []reflect.Value{}
	switch node.Name {
	case "range":
		j.beginRange++
		results = input
	case "end":
		if j.inRange > 0 {
			j.endRange++
		} else {
			return results, setFn, fmt.Errorf("not in range, nothing to end")
		}
	default:
		return input, setFn, fmt.Errorf("unrecognized identifier %v", node.Name)
	}
	return results, setFn, nil
}

// evalArray evaluates ArrayNode
func (j *jsonPathExtracter) evalArray(input []reflect.Value, node *jsonpath.ArrayNode, setFn []setFieldFunc) ([]reflect.Value, []setFieldFunc, error) {
	result := []reflect.Value{}
	nextFns := []setFieldFunc{}
	for k, value := range input {
		value, isNil := template.Indirect(value)
		if isNil {
			continue
		}
		if value.Kind() != reflect.Array && value.Kind() != reflect.Slice {
			return input, nextFns, fmt.Errorf("%v is not array or slice", value.Type())
		}
		params := node.Params
		if !params[0].Known {
			params[0].Value = 0
		}
		if params[0].Value < 0 {
			params[0].Value += value.Len()
		}
		if !params[1].Known {
			params[1].Value = value.Len()
		}

		if params[1].Value < 0 || (params[1].Value == 0 && params[1].Derived) {
			params[1].Value += value.Len()
		}
		sliceLength := value.Len()
		if params[1].Value != params[0].Value { // if you're requesting zero elements, allow it through.
			if params[0].Value >= sliceLength || params[0].Value < 0 {
				return input, nextFns, fmt.Errorf("array index out of bounds: index %d, length %d", params[0].Value, sliceLength)
			}
			if params[1].Value > sliceLength || params[1].Value < 0 {
				return input, nextFns, fmt.Errorf("array index out of bounds: index %d, length %d", params[1].Value-1, sliceLength)
			}
			if params[0].Value > params[1].Value {
				return input, nextFns, fmt.Errorf("starting index %d is greater than ending index %d", params[0].Value, params[1].Value)
			}
		} else {
			return result, nextFns, nil
		}

		value = value.Slice(params[0].Value, params[1].Value)

		step := 1
		if params[2].Known {
			if params[2].Value <= 0 {
				return input, nextFns, fmt.Errorf("step must be > 0")
			}
			step = params[2].Value
		}

		loopResult := []reflect.Value{}
		for i := 0; i < value.Len(); i += step {
			loopResult = append(loopResult, value.Index(i))
		}
		result = append(result, loopResult...)

		s := reflect.MakeSlice(value.Type(), len(loopResult), len(loopResult))
		for i := 0; i < len(loopResult); i++ {
			ii := i
			s.Index(ii).Set(loopResult[i])
			nextFns = append(nextFns, func(val reflect.Value) error {
				s.Index(ii).Set(val)
				return nil
			})
		}

		setFn[k](s) // nolint: errcheck
	}
	return result, nextFns, nil
}

func (j *jsonPathExtracter) _evalUnion(input []reflect.Value, node *jsonpath.UnionNode, setFn []setFieldFunc) ([]reflect.Value, []setFieldFunc, error) {
	result := []reflect.Value{}
	fns := []setFieldFunc{}

	union := make([][]reflect.Value, len(input))
	setFn_ := make([]setFieldFunc, len(input))

	for i := 0; i < len(input); i++ {
		ii := i
		setFn_[i] = func(val reflect.Value) error {
			union[ii] = append(union[ii], val)
			return nil
		}
	}

	for _, listNode := range node.Nodes {
		temp, nextFn, err := j._evalList(input, listNode, setFn_)
		if err != nil {
			return input, fns, err
		}
		result = append(result, temp...)
		fns = append(fns, nextFn...)
	}

	for i, fn := range setFn {
		if len(union[i]) == 0 {
			continue
		}

		m := union[i][0]
		for j := 1; j < len(union[i]); j++ {
			val := union[i][j]
			for _, key := range val.MapKeys() {
				m.SetMapIndex(key, val.MapIndex(key))
			}
		}
		fn(m) // nolint: errcheck
	}

	return result, fns, nil
}

// evalUnion evaluates UnionNode
func (j *jsonPathExtracter) evalUnion(input []reflect.Value, node *jsonpath.UnionNode) ([]reflect.Value, error) {
	result := []reflect.Value{}
	for _, listNode := range node.Nodes {
		temp, err := j.evalList(input, listNode)
		if err != nil {
			return input, err
		}
		result = append(result, temp...)
	}
	return result, nil
}

//lint:ignore U1000 ignore unused function
func (j *jsonPathExtracter) findFieldInValue(value *reflect.Value, node *jsonpath.FieldNode) (reflect.Value, error) {
	t := value.Type()
	var inlineValue *reflect.Value
	for ix := 0; ix < t.NumField(); ix++ {
		f := t.Field(ix)
		jsonTag := f.Tag.Get("json")
		parts := strings.Split(jsonTag, ",")
		if len(parts) == 0 {
			continue
		}
		if parts[0] == node.Value {
			return value.Field(ix), nil
		}
		if len(parts[0]) == 0 {
			val := value.Field(ix)
			inlineValue = &val
		}
	}
	if inlineValue != nil {
		if inlineValue.Kind() == reflect.Struct {
			// handle 'inline'
			match, err := j.findFieldInValue(inlineValue, node)
			if err != nil {
				return reflect.Value{}, err
			}
			if match.IsValid() {
				return match, nil
			}
		}
	}
	return value.FieldByName(node.Value), nil
}

// evalField evaluates field of struct or key of map.
func (j *jsonPathExtracter) evalField(input []reflect.Value, node *jsonpath.FieldNode, setFn []setFieldFunc) ([]reflect.Value, []setFieldFunc, error) {
	results := []reflect.Value{}
	nextFns := []setFieldFunc{}
	// If there's no input, there's no output
	if len(input) == 0 {
		return results, nextFns, nil
	}
	for k, value := range input {
		var result reflect.Value
		var fn setFieldFunc
		value, isNil := template.Indirect(value)
		if isNil {
			continue
		}

		if value.Kind() != reflect.Map {
			return results, nextFns, fmt.Errorf("%v is of the type %T, expected map[string]interface{}", value.Interface(), value.Interface())
		} else {
			mapKeyType := value.Type().Key()
			nodeValue := reflect.ValueOf(node.Value)
			// node value type must be convertible to map key type
			if !nodeValue.Type().ConvertibleTo(mapKeyType) {
				return results, nextFns, fmt.Errorf("%s is not convertible to %s", nodeValue, mapKeyType)
			}
			key := nodeValue.Convert(mapKeyType)
			result = value.MapIndex(key)

			val := reflect.MakeMap(value.Type())
			val.SetMapIndex(key, result)
			setFn[k](val) // nolint: errcheck

			fn = func(val_ reflect.Value) error {
				val.SetMapIndex(key, val_)
				return nil
			}
		}

		if result.IsValid() {
			results = append(results, result)
			nextFns = append(nextFns, fn)
		}
	}
	if len(results) == 0 {
		if j.allowMissingKeys {
			return results, nextFns, nil
		}
		return results, nextFns, fmt.Errorf("%s is not found", node.Value)
	}
	return results, nextFns, nil
}

// evalWildcard extracts all contents of the given value
func (j *jsonPathExtracter) evalWildcard(input []reflect.Value) ([]reflect.Value, error) {
	results := []reflect.Value{}
	for _, value := range input {
		value, isNil := template.Indirect(value)
		if isNil {
			continue
		}

		kind := value.Kind()
		if kind == reflect.Struct {
			for i := 0; i < value.NumField(); i++ {
				results = append(results, value.Field(i))
			}
		} else if kind == reflect.Map {
			for _, key := range value.MapKeys() {
				results = append(results, value.MapIndex(key))
			}
		} else if kind == reflect.Array || kind == reflect.Slice || kind == reflect.String {
			for i := 0; i < value.Len(); i++ {
				results = append(results, value.Index(i))
			}
		}
	}
	return results, nil
}

// evalRecursive visits the given value recursively and pushes all of them to result
func (j *jsonPathExtracter) evalRecursive(input []reflect.Value) ([]reflect.Value, error) {
	result := []reflect.Value{}
	for _, value := range input {
		results := []reflect.Value{}
		value, isNil := template.Indirect(value)
		if isNil {
			continue
		}

		kind := value.Kind()
		if kind == reflect.Struct {
			for i := 0; i < value.NumField(); i++ {
				results = append(results, value.Field(i))
			}
		} else if kind == reflect.Map {
			for _, key := range value.MapKeys() {
				results = append(results, value.MapIndex(key))
			}
		} else if kind == reflect.Array || kind == reflect.Slice || kind == reflect.String {
			for i := 0; i < value.Len(); i++ {
				results = append(results, value.Index(i))
			}
		}
		if len(results) != 0 {
			result = append(result, value)
			output, err := j.evalRecursive(results)
			if err != nil {
				return result, err
			}
			result = append(result, output...)
		}
	}
	return result, nil
}

// evalFilter filters array according to FilterNode
func (j *jsonPathExtracter) evalFilter(input []reflect.Value, node *jsonpath.FilterNode, setFn []setFieldFunc) ([]reflect.Value, []setFieldFunc, error) {
	results := []reflect.Value{}
	fns := []setFieldFunc{}
	for k, value := range input {
		value, _ = template.Indirect(value)

		if value.Kind() != reflect.Array && value.Kind() != reflect.Slice {
			return input, fns, fmt.Errorf("%v is not array or slice and cannot be filtered", value)
		}

		loopResult := []reflect.Value{}
		for i := 0; i < value.Len(); i++ {
			temp := []reflect.Value{value.Index(i)}
			lefts, err := j.evalList(temp, node.Left)

			// case exists
			if node.Operator == "exists" {
				if len(lefts) > 0 {
					results = append(results, value.Index(i))
				}
				continue
			}

			if err != nil {
				return input, fns, err
			}

			var left, right interface{}
			switch {
			case len(lefts) == 0:
				continue
			case len(lefts) > 1:
				return input, fns, fmt.Errorf("can only compare one element at a time")
			}
			left = lefts[0].Interface()

			rights, err := j.evalList(temp, node.Right)
			if err != nil {
				return input, fns, err
			}
			switch {
			case len(rights) == 0:
				continue
			case len(rights) > 1:
				return input, fns, fmt.Errorf("can only compare one element at a time")
			}
			right = rights[0].Interface()

			pass := false
			switch node.Operator {
			case "<":
				pass, err = template.Less(left, right)
			case ">":
				pass, err = template.Greater(left, right)
			case "==":
				pass, err = template.Equal(left, right)
			case "!=":
				pass, err = template.NotEqual(left, right)
			case "<=":
				pass, err = template.LessEqual(left, right)
			case ">=":
				pass, err = template.GreaterEqual(left, right)
			default:
				return results, fns, fmt.Errorf("unrecognized filter operator %s", node.Operator)
			}
			if err != nil {
				return results, fns, err
			}
			if pass {
				loopResult = append(loopResult, value.Index(i))
			}
		}

		s := reflect.MakeSlice(value.Type(), len(loopResult), len(loopResult))
		for i := 0; i < len(loopResult); i++ {
			ii := i
			s.Index(ii).Set(loopResult[i])
			fns = append(fns, func(val reflect.Value) error {
				s.Index(ii).Set(val)
				return nil
			})
		}

		setFn[k](s) // nolint:errcheck
		results = append(results, loopResult...)
	}
	return results, fns, nil
}
