// Copyright 2010 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mongo

import (
	"math"
	"os"
	"reflect"
	"runtime"
	"strconv"
)

var (
	typeOrderedMap = reflect.Typeof(OrderedMap{})
)

type EncodeTypeError struct {
	Type reflect.Type
}

func (e *EncodeTypeError) String() string {
	return "bson: unsupported type: " + e.Type.String()
}

type interfaceOrPtrValue interface {
	IsNil() bool
	Elem() reflect.Value
}

type encodeState struct {
	buffer
}

func Encode(buf []byte, doc interface{}) (result []byte, err os.Error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(os.Error)
		}
	}()

	v := reflect.NewValue(doc)
	if pv, ok := v.(interfaceOrPtrValue); ok {
		v = pv.Elem()
	}

	e := encodeState{buffer: buf}
	switch v := v.(type) {
	case *reflect.StructValue:
		e.writeStruct(v)
	case *reflect.MapValue:
		e.writeMap(v)
	default:
		if v.Type() == typeOrderedMap {
			e.writeOrderedMap(v.Interface().(OrderedMap))
		} else {
			return nil, &EncodeTypeError{v.Type()}
		}
	}
	return e.buffer, nil
}

func (e *encodeState) abort(err os.Error) {
	panic(err)
}

func (e *encodeState) beginDoc() (offset int) {
	offset = len(e.buffer)
	e.buffer.Next(4)
	return
}

func (e *encodeState) endDoc(offset int) {
	n := len(e.buffer) - offset
	wire.PutUint32(e.buffer[offset:offset+4], uint32(n))
}

func (e *encodeState) writeKindName(kind int, name string) {
	e.WriteByte(byte(kind))
	e.WriteString(name)
	e.WriteByte(0)
}

func (e *encodeState) writeStruct(v *reflect.StructValue) {
	offset := e.beginDoc()
	t := v.Type().(*reflect.StructType)
	for i := 0; i < v.NumField(); i++ {
		if name := fieldName(t.Field(i)); name != "" {
			e.encodeValue(name, v.Field(i))
		}
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) writeMap(v *reflect.MapValue) {
	if v.IsNil() {
		return
	}
	if _, ok := v.Type().(*reflect.MapType).Key().(*reflect.StringType); !ok {
		e.abort(&EncodeTypeError{v.Type()})
	}
	offset := e.beginDoc()
	for _, k := range v.Keys() {
		e.encodeValue(k.(*reflect.StringValue).Get(), v.Elem(k))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) writeOrderedMap(v OrderedMap) {
	offset := e.beginDoc()
	for _, kv := range v {
		e.encodeValue(kv.Key, reflect.NewValue(kv.Value))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) encodeValue(name string, value reflect.Value) {
	if value == nil {
		return
	}
	t := value.Type()
	encoder, found := typeEncoder[t]
	if !found {
		encoder, found = kindEncoder[t.Kind()]
		if !found {
			e.abort(&EncodeTypeError{value.Type()})
		}
	}
	encoder(e, name, value)
}

func encodeBool(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindBool, name)
	if value.(*reflect.BoolValue).Get() {
		e.WriteByte(1)
	} else {
		e.WriteByte(0)
	}
}

func encodeInt(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindInt32, name)
	e.WriteUint32(uint32(value.(*reflect.IntValue).Get()))
}

func encodeInt64(e *encodeState, kind int, name string, value reflect.Value) {
	e.writeKindName(kind, name)
	e.WriteUint64(uint64(value.(*reflect.IntValue).Get()))
}

func encodeFloat(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindFloat, name)
	e.WriteUint64(math.Float64bits(value.(*reflect.FloatValue).Get()))
}

func encodeString(e *encodeState, kind int, name string, value reflect.Value) {
	e.writeKindName(kind, name)
	s := value.(*reflect.StringValue).Get()
	e.WriteUint32(uint32(len(s) + 1))
	e.WriteString(s)
	e.WriteByte(0)
}

func encodeRegexp(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindRegexp, name)
	r := value.Interface().(Regexp)
	e.WriteString(r.Pattern)
	e.WriteByte(0)
	e.WriteString(r.Flags)
	e.WriteByte(0)
}

func encodeObjectId(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindObjectId, name)
	oid := value.Interface().(ObjectId)
	e.Write(oid[:])
}

func encodeCodeWithScope(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindCodeWithScope, name)
	c := value.Interface().(CodeWithScope)
	offset := e.beginDoc()
	e.WriteUint32(uint32(len(c.Code) + 1))
	e.WriteString(c.Code)
	e.WriteByte(0)
	scopeOffset := e.beginDoc()
	for k, v := range c.Scope {
		e.encodeValue(k, reflect.NewValue(v))
	}
	e.WriteByte(0)
	e.endDoc(scopeOffset)
	e.endDoc(offset)
}

func encodeKey(e *encodeState, name string, value reflect.Value) {
	switch value.Interface().(Key) {
	case 1:
		e.writeKindName(kindMaxKey, name)
	case -1:
		e.writeKindName(kindMinKey, name)
	default:
		e.abort(os.NewError("bson: unknown key"))
	}
}

func encodeStruct(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindDocument, name)
	e.writeStruct(value.(*reflect.StructValue))
}

func encodeMap(e *encodeState, name string, value reflect.Value) {
	v := value.(*reflect.MapValue)
	if !v.IsNil() {
		e.writeKindName(kindDocument, name)
		e.writeMap(v)
	}
}

func encodeOrderedMap(e *encodeState, name string, value reflect.Value) {
	v := value.Interface().(OrderedMap)
	if v != nil {
		e.writeKindName(kindDocument, name)
		e.writeOrderedMap(v)
	}
}

func encodeByteSlice(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindBinary, name)
	b := value.Interface().([]byte)
	e.WriteUint32(uint32(len(b)))
	e.WriteByte(0)
	e.Write(b)
}

func encodeArrayOrSlice(e *encodeState, name string, value reflect.Value) {
	e.writeKindName(kindArray, name)
	offset := e.beginDoc()
	v := value.(reflect.ArrayOrSliceValue)
	n := v.Len()
	for i := 0; i < n; i++ {
		e.encodeValue(strconv.Itoa(i), v.Elem(i))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func encodeInterfaceOrPtr(e *encodeState, name string, value reflect.Value) {
	v := value.(interfaceOrPtrValue)
	if !v.IsNil() {
		e.encodeValue(name, v.Elem())
	}
}

type encoderFunc func(e *encodeState, name string, v reflect.Value)

var kindEncoder map[reflect.Kind]encoderFunc
var typeEncoder map[reflect.Type]encoderFunc

func init() {
	kindEncoder = map[reflect.Kind]encoderFunc{
		reflect.Array:     encodeArrayOrSlice,
		reflect.Bool:      encodeBool,
		reflect.Float32:   encodeFloat,
		reflect.Float64:   encodeFloat,
		reflect.Float:     encodeFloat,
		reflect.Int32:     encodeInt,
		reflect.Int64:     func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, kindInt64, name, value) },
		reflect.Int:       func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, kindInt64, name, value) },
		reflect.Interface: encodeInterfaceOrPtr,
		reflect.Map:       encodeMap,
		reflect.Ptr:       encodeInterfaceOrPtr,
		reflect.Slice:     encodeArrayOrSlice,
		reflect.String:    func(e *encodeState, name string, value reflect.Value) { encodeString(e, kindString, name, value) },
		reflect.Struct:    encodeStruct,
	}
	typeEncoder = map[reflect.Type]encoderFunc{
		reflect.Typeof([]byte{}):        encodeByteSlice,
		reflect.Typeof(Code("")):        func(e *encodeState, name string, value reflect.Value) { encodeString(e, kindCode, name, value) },
		reflect.Typeof(CodeWithScope{}): encodeCodeWithScope,
		reflect.Typeof(DateTime(0)):     func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, kindDateTime, name, value) },
		reflect.Typeof(MaxKey):          encodeKey,
		reflect.Typeof(ObjectId{}):      encodeObjectId,
		typeOrderedMap:                  encodeOrderedMap,
		reflect.Typeof(Regexp{}):        encodeRegexp,
		reflect.Typeof(Symbol("")):      func(e *encodeState, name string, value reflect.Value) { encodeString(e, kindSymbol, name, value) },
		reflect.Typeof(Timestamp(0)):    func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, kindTimestamp, name, value) },
	}
}
