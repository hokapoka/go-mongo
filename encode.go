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
	"bytes"
	"math"
	"os"
	"reflect"
	"runtime"
	"strconv"
)

type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) String() string {
	return "bson: unsupported type: " + e.Type.String()
}

type interfaceOrPtrValue interface {
	IsNil() bool
	Elem() reflect.Value
}

type encodeState struct {
	*bytes.Buffer
	buf [16]byte
}

func Encode(buf *bytes.Buffer, doc interface{}) (err os.Error) {
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

	e := encodeState{Buffer: buf}
	switch v := v.(type) {
	case *reflect.StructValue:
		e.writeStruct(v)
	case *reflect.MapValue:
		e.writeMap(v)
	default:
		return &UnsupportedTypeError{v.Type()}
	}
	return nil
}

func (e *encodeState) error(err os.Error) {
	panic(err)
}

func (e *encodeState) beginDoc() (offset int) {
	offset = len(e.Bytes())
	e.Write(e.buf[:4]) // placeholder
	return
}

func (e *encodeState) endDoc(offset int) {
	n := len(e.Bytes()) - offset
	wire.PutUint32(e.Bytes()[offset:offset+4], uint32(n))
}

func (e *encodeState) writeStruct(v *reflect.StructValue) {
	offset := e.beginDoc()
	t := v.Type().(*reflect.StructType)
	for i := 0; i < v.NumField(); i++ {
		e.encodeValue(t.Field(i).Name, v.Field(i))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) writeMap(v *reflect.MapValue) {
	if _, ok := v.Type().(*reflect.MapType).Key().(*reflect.StringType); !ok {
		e.error(&UnsupportedTypeError{v.Type()})
	}
	if v.IsNil() {
		// bufX null
		return
	}
	offset := e.beginDoc()
	for _, k := range v.Keys() {
		e.encodeValue(k.(*reflect.StringValue).Get(), v.Elem(k))
	}
	e.WriteByte(0)
	e.endDoc(offset)
}

func (e *encodeState) encodeValue(name string, value reflect.Value) {
	if value == nil {
		panic(os.NewError("nil not handled"))
	}
	t := value.Type()
	encoder, found := typeEncoder[t]
	if !found {
		encoder, found = kindEncoder[t.Kind()]
		if !found {
			e.error(&UnsupportedTypeError{value.Type()})
		}
	}
	encoder(e, name, value)
}

func encodeBool(e *encodeState, name string, value reflect.Value) {
	v := value.(*reflect.BoolValue)
	e.WriteByte(0x8)
	e.WriteString(name)
	e.WriteByte(0)
	if v.Get() {
		e.WriteByte(1)
	} else {
		e.WriteByte(0)
	}
}

func encodeInt(e *encodeState, name string, value reflect.Value) {
	v := value.(*reflect.IntValue)
	e.WriteByte(0x10)
	e.WriteString(name)
	e.WriteByte(0)
	wire.PutUint32(e.buf[:4], uint32(v.Get()))
	e.Write(e.buf[:4])
}

func encodeInt64(e *encodeState, tag byte, name string, value reflect.Value) {
	v := value.(*reflect.IntValue)
	e.WriteByte(tag)
	e.WriteString(name)
	e.WriteByte(0)
	wire.PutUint64(e.buf[:8], uint64(v.Get()))
	e.Write(e.buf[:8])
}

func encodeFloat(e *encodeState, name string, value reflect.Value) {
	v := value.(*reflect.FloatValue)
	e.WriteByte(0x1)
	e.WriteString(name)
	e.WriteByte(0)
	wire.PutUint64(e.buf[:8], math.Float64bits(v.Get()))
	e.Write(e.buf[:8])
}

func encodeString(e *encodeState, tag byte, name string, value reflect.Value) {
	v := value.(*reflect.StringValue)
	e.WriteByte(tag)
	e.WriteString(name)
	e.WriteByte(0)
	s := v.Get()
	wire.PutUint32(e.buf[:4], uint32(len(s)+1))
	e.Write(e.buf[:4])
	e.WriteString(s)
	e.WriteByte(0)
}

func encodeRegexp(e *encodeState, name string, value reflect.Value) {
	r := value.Interface().(Regexp)
	e.WriteByte(0xb)
	e.WriteString(name)
	e.WriteByte(0)
	e.WriteString(r.Pattern)
	e.WriteByte(0)
	e.WriteString(r.Flags)
	e.WriteByte(0)
}

func encodeObjectId(e *encodeState, name string, value reflect.Value) {
	oid := value.Interface().(ObjectId)
	e.WriteByte(0x7)
	e.WriteString(name)
	e.WriteByte(0)
	e.Write(oid[:])
}

func encodeCodeWithScope(e *encodeState, name string, value reflect.Value) {
	c := value.Interface().(CodeWithScope)
	e.WriteByte(0xf)
	e.WriteString(name)
	e.WriteByte(0)
	offset := e.beginDoc()
	wire.PutUint32(e.buf[:4], uint32(len(c.Code)+1))
	e.Write(e.buf[:4])
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

func encodeStruct(e *encodeState, name string, value reflect.Value) {
	v := value.(*reflect.StructValue)
	e.WriteByte(0x3)
	e.WriteString(name)
	e.WriteByte(0)
	e.writeStruct(v)
}

func encodeMap(e *encodeState, name string, value reflect.Value) {
	v := value.(*reflect.MapValue)
	if v.IsNil() {
		e.WriteByte(0xa)
		e.WriteString(name)
		e.WriteByte(0)
	} else {
		e.WriteByte(0x3)
		e.WriteString(name)
		e.WriteByte(0)
		e.writeMap(v)
	}
}

func encodeArrayOrSlice(e *encodeState, name string, value reflect.Value) {
	t := value.Type().(reflect.ArrayOrSliceType)
	if t.Elem().Kind() == reflect.Uint8 {
		b := value.Interface().([]byte)
		e.WriteByte(0x5)
		e.WriteString(name)
		e.WriteByte(0)
		wire.PutUint32(e.buf[:4], uint32(len(b)))
		e.Write(e.buf[:4])
		e.WriteByte(0)
		e.Write(b)
	} else {
		v := value.(reflect.ArrayOrSliceValue)
		e.WriteByte(0x4)
		e.WriteString(name)
		e.WriteByte(0)
		offset := e.beginDoc()
		n := v.Len()
		for i := 0; i < n; i++ {
			e.encodeValue(strconv.Itoa(i), v.Elem(i))
		}
		e.WriteByte(0)
		e.endDoc(offset)
	}
}

func encodeInterfaceOrPtr(e *encodeState, name string, value reflect.Value) {
	v := value.(interfaceOrPtrValue)
	if v.IsNil() {
		e.WriteByte(0xa)
		e.WriteString(name)
		e.WriteByte(0)
		return
	}
	e.encodeValue(name, v.Elem())
}

type encoderFunc func(e *encodeState, name string, v reflect.Value)

var kindEncoder map[reflect.Kind]encoderFunc
var typeEncoder map[reflect.Type]encoderFunc

func init() {
	kindEncoder = map[reflect.Kind]encoderFunc{
		reflect.Struct:    encodeStruct,
		reflect.Ptr:       encodeInterfaceOrPtr,
		reflect.Interface: encodeInterfaceOrPtr,
		reflect.String:    func(e *encodeState, name string, value reflect.Value) { encodeString(e, 0x2, name, value) },
		reflect.Int8:      encodeInt,
		reflect.Int16:     encodeInt,
		reflect.Int32:     encodeInt,
		reflect.Int:       encodeInt,
		reflect.Int64:     func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, 0x12, name, value) },
		reflect.Float:     encodeFloat,
		reflect.Float32:   encodeFloat,
		reflect.Float64:   encodeFloat,
		reflect.Bool:      encodeBool,
		reflect.Array:     encodeArrayOrSlice,
		reflect.Slice:     encodeArrayOrSlice,
		reflect.Map:       encodeMap,
	}
	typeEncoder = map[reflect.Type]encoderFunc{
		reflect.Typeof(DateTime(0)):     func(e *encodeState, name string, value reflect.Value) { encodeInt64(e, 0x9, name, value) },
		reflect.Typeof(Regexp{}):        encodeRegexp,
		reflect.Typeof(ObjectId{}):      encodeObjectId,
		reflect.Typeof(CodeWithScope{}): encodeCodeWithScope,
	}
}
