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
	typeDoc      = reflect.Typeof(Doc{})
	typeBSONData = reflect.Typeof(BSONData{})
)

// EncodeTypeError is the error indicating that Encode could not encode an input type.
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

// Encode appends the BSON encoding of doc to buf and returns the new slice.
//
// Encode traverses the value doc recursively using the following
// type-dependent encodings:
//
// Struct values encode as BSON documents. Each struct field becomes an element
// of the document using the field name as the element key. If the struct field
// has a tag, then the tag is used as the element name. Only exported names are
// encoded.  
//
// Array and slice values encode as BSON arrays.
//
// Map values encode as BSON documents. The map's key type must be string; the
// object keys are used directly as map keys.
//
// Pointer values encode as the value pointed to. A nil pointer encodes as a BSON null.
//
// Interface values encode as the value contained in the interface. A nil
// interface value encodes as BSON null.
// 
// Other types are encoded as follows
//
//      Go                  -> BSON
//      bool                -> Boolean
//      float32             -> Double
//      float64             -> Double
//      int32               -> 32-bit Integer
//      int                 -> 32-bit Integer
//      int64               -> 64-bit Integer
//      string              -> String
//      []byte              -> Binary data
//      mongo.Code          -> Javascript code
//      mongo.CodeWithScope -> Javascript code with scope
//      mongo.DateTime      -> UTC Datetime
//      mongo.Doc           -> Document. Use when element order is important.
//      mongo.MinMax        -> Minimum / Maximum value
//      mongo.ObjectId      -> ObjectId
//      mongo.Regexp        -> Regular expression
//      mongo.Symbol        -> Symbol
//      mongo.Timestamp     -> Timestamp
//
// Other types including channels, complex and function values cannot be encoded.
//
// BSON cannot represent cyclic data structurs and Encode does not handle them.
// Passing cyclic structures to Encode will result in an infinite recursion.
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
	switch v.Type() {
	case typeDoc:
		e.writeDoc(v.Interface().(Doc))
	case typeBSONData:
		rd := v.Interface().(BSONData)
		if rd.Kind != kindDocument {
			return nil, &EncodeTypeError{v.Type()}
		}
		e.Write(rd.Data)
	default:
		switch v := v.(type) {
		case *reflect.StructValue:
			e.writeStruct(v)
		case *reflect.MapValue:
			e.writeMap(v)
		default:
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
	for name, f := range compileStruct(v.Type().(*reflect.StructType)) {
		e.encodeValue(name, v.FieldByIndex(f.Index))
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

func (e *encodeState) writeDoc(v Doc) {
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
	e.WriteString(r.Options)
	e.WriteByte(0)
}

func encodeObjectId(e *encodeState, name string, value reflect.Value) {
	oid := value.Interface().(ObjectId)
	skip := true
	for i := 0; i < len(oid); i++ {
		if oid[i] != 0 {
			skip = false
			break
		}
	}
	if skip {
		return
	}
	e.writeKindName(kindObjectId, name)
	e.Write(oid[:])
}

func encodeBSONData(e *encodeState, name string, value reflect.Value) {
	rd := value.Interface().(BSONData)
	e.writeKindName(rd.Kind, name)
	e.Write(rd.Data)
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

func encodeMinMax(e *encodeState, name string, value reflect.Value) {
	switch value.Interface().(MinMax) {
	case 1:
		e.writeKindName(kindMaxValue, name)
	case -1:
		e.writeKindName(kindMinValue, name)
	default:
		e.abort(os.NewError("bson: unknown MinMax value"))
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

func encodeDoc(e *encodeState, name string, value reflect.Value) {
	v := value.Interface().(Doc)
	if v != nil {
		e.writeKindName(kindDocument, name)
		e.writeDoc(v)
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
		reflect.Int32:     encodeInt,
		reflect.Int64:     func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, kindInt64, name, value) },
		reflect.Int:       encodeInt,
		reflect.Interface: encodeInterfaceOrPtr,
		reflect.Map:       encodeMap,
		reflect.Ptr:       encodeInterfaceOrPtr,
		reflect.Slice:     encodeArrayOrSlice,
		reflect.String:    func(e *encodeState, name string, value reflect.Value) { encodeString(e, kindString, name, value) },
		reflect.Struct:    encodeStruct,
	}
	typeEncoder = map[reflect.Type]encoderFunc{
		typeDoc:                         encodeDoc,
		typeBSONData:                    encodeBSONData,
		reflect.Typeof(Code("")):        func(e *encodeState, name string, value reflect.Value) { encodeString(e, kindCode, name, value) },
		reflect.Typeof(CodeWithScope{}): encodeCodeWithScope,
		reflect.Typeof(DateTime(0)):     func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, kindDateTime, name, value) },
		reflect.Typeof(MinMax(0)):       encodeMinMax,
		reflect.Typeof(ObjectId{}):      encodeObjectId,
		reflect.Typeof(Regexp{}):        encodeRegexp,
		reflect.Typeof(Symbol("")):      func(e *encodeState, name string, value reflect.Value) { encodeString(e, kindSymbol, name, value) },
		reflect.Typeof(Timestamp(0)):    func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, kindTimestamp, name, value) },
		reflect.Typeof([]byte{}):        encodeByteSlice,
	}
}
