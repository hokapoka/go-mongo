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
)

var (
	ErrEOD = os.NewError("bson: unexpected end of data")
)

type DecodeConvertError struct {
	kind int
	t    reflect.Type
}

func (e *DecodeConvertError) String() string {
	return "bson: could not decode " + kindName(e.kind) + " to " + e.t.String()
}

type DecodeTypeError struct {
	kind int
}

func (e *DecodeTypeError) String() string {
	return "bson: could not decode " + kindName(e.kind)
}

func Decode(data []byte, v interface{}) (err os.Error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(os.Error)
		}
	}()

	d := decodeState{data: data}

	pv, ok := reflect.NewValue(v).(*reflect.PtrValue)
	if !ok || pv.IsNil() {
		return os.NewError("bson: decode arg must be pointer")
	}
	d.decodeValue(kindDocument, pv.Elem())
	return d.savedError
}

// decodeState represents the state while decoding a JSON value.
type decodeState struct {
	data       []byte
	offset     int // read offset in data
	savedError os.Error
}

// abort by panicking with err.
func (d *decodeState) abort(err os.Error) {
	panic(err)
}

// saveError saves the first err it is called with, for reporting at the end of
// Decode.
func (d *decodeState) saveError(err os.Error) {
	if d.savedError == nil {
		d.savedError = err
	}
}

// saveErrorAndSkip skips the value and saves a conversion error.
func (d *decodeState) saveErrorAndSkip(kind int, t reflect.Type) {
	d.skipValue(kind)
	if d.savedError == nil {
		d.savedError = &DecodeConvertError{kind, t}
	}
}

func (d *decodeState) compileStruct(t *reflect.StructType) map[string]reflect.StructField {
	m := make(map[string]reflect.StructField)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if name := fieldName(f); name != "" {
			m[name] = f
		}
	}
	return m
}

func (d *decodeState) beginDoc() int {
	offset := d.offset
	offset += int(wire.Uint32(d.scanSlice(4)))
	return offset
}

func (d *decodeState) endDoc(offset int) {
	if d.offset != offset {
		d.abort(os.NewError("bson: doc length wrong"))
	}
}

func (d *decodeState) scanSlice(n int) []byte {
	if n+d.offset > len(d.data) {
		d.abort(ErrEOD)
	}
	p := d.data[d.offset : d.offset+n]
	d.offset += n
	return p
}

func (d *decodeState) scanKindName() (int, []byte) {
	if d.offset >= len(d.data) {
		d.abort(ErrEOD)
	}
	kind := int(d.data[d.offset])
	d.offset += 1
	if kind == 0 {
		return 0, nil
	}
	n := bytes.IndexByte(d.data[d.offset:], 0)
	if n < 0 {
		d.abort(ErrEOD)
	}
	p := d.data[d.offset : d.offset+n]
	d.offset = d.offset + n + 1
	return kind, p
}

func (d *decodeState) scanFloat() float64 {
	return math.Float64frombits(wire.Uint64(d.scanSlice(8)))
}

func (d *decodeState) scanString() string {
	n := int(wire.Uint32(d.scanSlice(4)))
	s := string(d.scanSlice(n - 1))
	d.offset += 1 // skip null terminator
	return s
}

func (d *decodeState) scanBinary() ([]byte, int) {
	n := int(wire.Uint32(d.scanSlice(4)))
	subtype := int(d.scanSlice(1)[0])
	return d.scanSlice(n), subtype
}

func (d *decodeState) scanObjectId() []byte {
	return d.scanSlice(12)
}

func (d *decodeState) scanBool() bool {
	b := d.scanSlice(1)[0]
	if b == 0 {
		return false
	}
	return true
}

func (d *decodeState) scanInt32() int32 {
	return int32(wire.Uint32(d.scanSlice(4)))
}

func (d *decodeState) scanInt64() int64 {
	return int64(wire.Uint64(d.scanSlice(8)))
}

func (d *decodeState) decodeValue(kind int, v reflect.Value) {
	v = d.indirect(v)
	t := v.Type()
	decoder, ok := typeDecoder[t]
	if !ok {
		decoder, ok = kindDecoder[t.Kind()]
		if !ok {
			d.saveErrorAndSkip(kind, v.Type())
			return
		}
	}
	decoder(d, kind, v)
}

// indirect walks down v allocating pointers as needed, until it gets to a
// non-pointer.  
func (d *decodeState) indirect(v reflect.Value) reflect.Value {
	for {
		/*
			if iv, ok := v.(*reflect.InterfaceValue); ok && !iv.IsNil() {
				v = iv.Elem()
				continue
			}
		*/
		pv, ok := v.(*reflect.PtrValue)
		if !ok {
			break
		}
		if pv.IsNil() {
			pv.PointTo(reflect.MakeZero(pv.Type().(*reflect.PtrType).Elem()))
		}
		v = pv.Elem()
	}
	return v
}

func decodeFloat(d *decodeState, kind int, value reflect.Value) {
	var f float64
	switch kind {
	default:
		d.saveErrorAndSkip(kind, value.Type())
		return
	case kindFloat:
		f = d.scanFloat()
	case kindInt64:
		f = float64(d.scanInt64())
	case kindInt32:
		f = float64(d.scanInt32())
	}
	v := value.(*reflect.FloatValue)
	if v.Overflow(f) {
		d.saveError(&DecodeConvertError{kind, v.Type()})
		return
	}
	v.Set(f)
}

func decodeInt(d *decodeState, kind int, value reflect.Value) {
	var n int64
	switch kind {
	default:
		d.saveErrorAndSkip(kind, value.Type())
		return
	case kindInt64, kindTimestamp, kindDateTime:
		n = d.scanInt64()
	case kindInt32:
		n = int64(d.scanInt32())
	case kindFloat:
		n = int64(d.scanFloat())
	}
	v := value.(*reflect.IntValue)
	if v.Overflow(n) {
		d.saveError(&DecodeConvertError{kind, v.Type()})
		return
	}
	v.Set(n)
}

func decodeTimestamp(d *decodeState, kind int, value reflect.Value) {
	switch kind {
	default:
		d.saveErrorAndSkip(kind, value.Type())
		return
	case kindInt64, kindTimestamp:
		decodeInt(d, kindInt64, value)
	}
}

func decodeDateTime(d *decodeState, kind int, value reflect.Value) {
	switch kind {
	default:
		d.saveErrorAndSkip(kind, value.Type())
		return
	case kindInt64, kindDateTime:
		decodeInt(d, kindInt64, value)
	}
}

func decodeString(d *decodeState, kind int, value reflect.Value) {
	var s string
	switch kind {
	default:
		d.saveErrorAndSkip(kind, value.Type())
		return
	case kindString, kindSymbol, kindCode:
		s = d.scanString()
	}
	value.(*reflect.StringValue).Set(s)
}

func decodeObjectId(d *decodeState, kind int, value reflect.Value) {
	var p []byte
	switch kind {
	default:
		d.saveErrorAndSkip(kind, value.Type())
		return
	case kindObjectId:
		p = d.scanSlice(12)
	}
	reflect.ArrayCopy(value.(reflect.ArrayOrSliceValue), reflect.NewValue(p).(reflect.ArrayOrSliceValue))
}

func decodeByteSlice(d *decodeState, kind int, value reflect.Value) {
	var p []byte
	switch kind {
	default:
		d.saveErrorAndSkip(kind, value.Type())
		return
	case kindBinary:
		p, _ = d.scanBinary()
	}
	v := value.(*reflect.SliceValue)
	if v.IsNil() || v.Cap() < len(p) {
		v.Set(reflect.MakeSlice(v.Type().(*reflect.SliceType), len(p), len(p)))
	} else {
		v.SetLen(len(p))
	}
	reflect.ArrayCopy(v, reflect.NewValue(p).(reflect.ArrayOrSliceValue))
}

func decodeBool(d *decodeState, kind int, value reflect.Value) {
	var b bool
	switch kind {
	default:
		d.saveErrorAndSkip(kind, value.Type())
		return
	case kindBool:
		b = d.scanBool()
	case kindInt32:
		b = d.scanInt32() != 0
	case kindInt64:
		b = d.scanInt64() != 0
	}
	value.(*reflect.BoolValue).Set(b)
}

func decodeKey(d *decodeState, kind int, value reflect.Value) {
	var n int64
	switch kind {
	default:
		d.saveError(&DecodeConvertError{kind, value.Type()})
		return
	case kindMaxKey:
		n = 1
	case kindMinKey:
		n = -1
	}
	value.(*reflect.IntValue).Set(n)
}

func decodeMapStringInterface(d *decodeState, kind int, value reflect.Value) {
	if kind != kindDocument {
		d.saveErrorAndSkip(kind, value.Type())
	}
	v := value.(*reflect.MapValue)
	if v.IsNil() {
		t := v.Type().(*reflect.MapType)
		v.SetValue(reflect.MakeMap(t))
	}
	m := v.Interface().(map[string]interface{})
	offset := d.beginDoc()
	for {
		kind, name := d.scanKindName()
		if kind == 0 {
			break
		}
		m[string(name)] = d.decodeValueInterface(kind)
	}
	d.endDoc(offset)
}

func decodeMap(d *decodeState, kind int, value reflect.Value) {
	t := value.Type().(*reflect.MapType)
	if t.Key().Kind() != reflect.String || kind != kindDocument {
		d.saveErrorAndSkip(kind, value.Type())
		return
	}
	v := value.(*reflect.MapValue)
	if v.IsNil() {
		v.SetValue(reflect.MakeMap(t))
	}
	offset := d.beginDoc()
	for {
		kind, name := d.scanKindName()
		if kind == 0 {
			break
		}
		subv := reflect.MakeZero(t.Elem())
		d.decodeValue(kind, subv)
		v.SetElem(reflect.NewValue(string(name)), subv)
	}
	d.endDoc(offset)
}

func decodeSlice(d *decodeState, kind int, value reflect.Value) {
	if kind != kindArray {
		d.saveErrorAndSkip(kind, value.Type())
		return
	}
	v := value.(*reflect.SliceValue)
	t := v.Type().(*reflect.SliceType)
	offset := d.beginDoc()
	i := 0
	for {
		kind, _ := d.scanKindName()
		if kind == 0 {
			break
		}
		if i >= v.Cap() {
			newcap := v.Cap() + v.Cap()/2
			if newcap < 4 {
				newcap = 4
			}
			newv := reflect.MakeSlice(t, v.Len(), newcap)
			reflect.ArrayCopy(newv, v)
			v.Set(newv)
		}
		if i >= v.Len() {
			v.SetLen(i + 1)
		}
		d.decodeValue(kind, v.Elem(i))
		i += 1
	}
	d.endDoc(offset)
}

func decodeArray(d *decodeState, kind int, value reflect.Value) {
	if kind != kindArray {
		d.saveErrorAndSkip(kind, value.Type())
		return
	}
	v := value.(*reflect.ArrayValue)
	offset := d.beginDoc()
	i := 0
	for {
		kind, _ := d.scanKindName()
		if kind == 0 {
			break
		}
		if i < v.Len() {
			d.decodeValue(kind, v.Elem(i))
		} else {
			d.skipValue(kind)
		}
		i += 1
	}
	d.endDoc(offset)
}

func decodeStruct(d *decodeState, kind int, value reflect.Value) {
	v := value.(*reflect.StructValue)
	t := v.Type().(*reflect.StructType)
	m := d.compileStruct(t)
	offset := d.beginDoc()
	for {
		kind, name := d.scanKindName()
		if kind == 0 {
			break
		}
		if kind == kindNull {
			continue
		}
		if f, ok := m[string(name)]; ok {
			d.decodeValue(kind, v.FieldByIndex(f.Index))
		} else {
			d.skipValue(kind)
		}
	}
	d.endDoc(offset)
}

func decodeInterface(d *decodeState, kind int, value reflect.Value) {
	value.(*reflect.InterfaceValue).Set(reflect.NewValue(d.decodeValueInterface(kind)))
}

func (d *decodeState) decodeValueInterface(kind int) interface{} {
	switch kind {
	case kindFloat:
		return d.scanFloat()
	case kindString:
		return d.scanString()
	case kindDocument:
		m := make(map[string]interface{})
		offset := d.beginDoc()
		for {
			kind, name := d.scanKindName()
			if kind == 0 {
				break
			}
			m[string(name)] = d.decodeValueInterface(kind)
		}
		d.endDoc(offset)
		return m
	case kindArray:
		var a []interface{}
		offset := d.beginDoc()
		for {
			kind, _ := d.scanKindName()
			if kind == 0 {
				break
			}
			a = append(a, d.decodeValueInterface(kind))
		}
		d.endDoc(offset)
		return a
	case kindBinary:
		p, _ := d.scanBinary()
		newp := make([]byte, len(p))
		copy(newp, p)
		return newp
	case kindObjectId:
		p := d.scanSlice(12)
		oid := ObjectId{}
		copy(oid[:], p)
		return oid
	case kindBool:
		return d.scanBool()
	case kindDateTime:
		return DateTime(d.scanInt64())
	case kindNull:
		return nil
	case kindSymbol:
		return Symbol(d.scanString())
	case kindInt32:
		return d.scanInt32()
	case kindTimestamp:
		return Timestamp(d.scanInt64())
	case kindInt64:
		return d.scanInt64()
	case kindMinKey:
		return MinKey
	case kindMaxKey:
		return MaxKey
	default:
		d.abort(&DecodeTypeError{kind})
	}
	return nil
}

func (d *decodeState) skipValue(kind int) {
	switch kind {
	case kindString, kindSymbol:
		n := int(d.scanInt32())
		d.offset += n
	case kindDocument, kindArray:
		n := int(d.scanInt32())
		d.offset += n - 4
	case kindBinary:
		n := int(d.scanInt32())
		d.offset += n + 1
	case kindObjectId:
		d.offset += 12
	case kindBool:
		d.offset += 1
	case kindDateTime, kindTimestamp, kindInt64, kindFloat:
		d.offset += 8
	case kindInt32:
		d.offset += 4
	case kindMinKey, kindMaxKey, kindNull:
		d.offset += 0
	default:
		d.abort(&DecodeTypeError{kind})
	}
}

type decoderFunc func(e *decodeState, kind int, v reflect.Value)

var kindDecoder map[reflect.Kind]decoderFunc
var typeDecoder map[reflect.Type]decoderFunc

func init() {
	kindDecoder = map[reflect.Kind]decoderFunc{
		reflect.Bool:      decodeBool,
		reflect.Float32:   decodeFloat,
		reflect.Float64:   decodeFloat,
		reflect.Float:     decodeFloat,
		reflect.Int32:     decodeInt,
		reflect.Int64:     decodeInt,
		reflect.Int:       decodeInt,
		reflect.Interface: decodeInterface,
		reflect.Map:       decodeMap,
		reflect.String:    decodeString,
		reflect.Struct:    decodeStruct,
		reflect.Slice:     decodeSlice,
		reflect.Array:     decodeArray,
	}
	typeDecoder = map[reflect.Type]decoderFunc{
		reflect.Typeof([]byte{}):                     decodeByteSlice,
		reflect.Typeof(DateTime(0)):                  decodeDateTime,
		reflect.Typeof(MaxKey):                       decodeKey,
		reflect.Typeof(make(map[string]interface{})): decodeMapStringInterface,
		reflect.Typeof(ObjectId{}):                   decodeObjectId,
		reflect.Typeof(Symbol("")):                   decodeString,
		reflect.Typeof(Timestamp(0)):                 decodeTimestamp,
	}
}
