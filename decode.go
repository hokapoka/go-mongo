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

type DecodeTypeError struct {
	src  string
	dest reflect.Type
}

func (d *DecodeTypeError) String() string {
	return "bson: could not decode " + d.src + " to " + d.dest.String()
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
	d.decodeDocument(d.indirect(pv.Elem(), false))
	return d.savedError
}

// decodeState represents the state while decoding a JSON value.
type decodeState struct {
	data       []byte
	offset     int // read offset in data
	savedError os.Error
}

// error aborts the decoding by panicking with err.
func (d *decodeState) error(err os.Error) {
	panic(err)
}

// saveError saves the first err it is called with,
// for reporting at the end of the unmarshal.
func (d *decodeState) saveError(err os.Error) {
	if d.savedError == nil {
		d.savedError = err
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

func (d *decodeState) scanSlice(n int) []byte {
	if n+d.offset > len(d.data) {
		d.error(ErrEOD)
	}
	p := d.data[d.offset : d.offset+n]
	d.offset += n
	return p
}

func (d *decodeState) beginDoc() int {
	offset := d.offset
	offset += int(wire.Uint32(d.scanSlice(4)))
	return offset
}

func (d *decodeState) endDoc(offset int) {
	if d.offset != offset {
		d.error(os.NewError("bson: doc length wrong"))
	}
}

func (d *decodeState) scanKindName() (int, []byte) {
	if d.offset >= len(d.data) {
		d.error(ErrEOD)
	}
	kind := int(d.data[d.offset])
	d.offset += 1
	if kind == 0 {
		return 0, nil
	}
	n := bytes.IndexByte(d.data[d.offset:], 0)
	if n < 0 {
		d.error(ErrEOD)
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

func (d *decodeState) decodeDocument(v reflect.Value) {
	switch v := v.(type) {
	case *reflect.InterfaceValue:
		m := make(map[string]interface{})
		v.Set(reflect.NewValue(m))
		d.decodeDocumentInterface(m)
	case *reflect.MapValue:
		d.decodeDocumentMap(v)
	case *reflect.StructValue:
		d.decodeDocumentStruct(v)
	default:
		d.saveError(&DecodeTypeError{"document", v.Type()})
	}
}

func (d *decodeState) decodeDocumentMap(v *reflect.MapValue) {
	offset := d.beginDoc()
	t := v.Type().(*reflect.MapType)
	if t.Key() != typeString {
		d.saveError(&DecodeTypeError{"object", v.Type()})
		return
	}
	if v.IsNil() {
		v.SetValue(reflect.MakeMap(t))
	}
	if t.Elem() == typeEmptyInterface {
		d.decodeDocumentInterface(v.Interface().(map[string]interface{}))
	} else {
		for {
			kind, name := d.scanKindName()
			if kind == 0 {
				break
			}
			subv := reflect.MakeZero(t.Elem())
			d.decodeValue(kind, subv)
			v.SetElem(reflect.NewValue(string(name)), subv)
		}
	}
	d.endDoc(offset)
}

func (d *decodeState) decodeDocumentStruct(v *reflect.StructValue) {
	offset := d.beginDoc()
	m := d.compileStruct(v.Type().(*reflect.StructType))
	for {
		kind, p := d.scanKindName()
		if kind == 0 {
			break
		}
		name := string(p)
		if f, ok := m[name]; ok {
			subv := v.FieldByIndex(f.Index)
			d.decodeValue(kind, subv)
		} else {
			d.skipValue(kind)
		}
	}
	d.endDoc(offset)
}

func (d *decodeState) decodeDocumentInterface(m map[string]interface{}) {
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

func (d *decodeState) decodeValue(kind int, v reflect.Value) {
	v = d.indirect(v, kind == kindNull)

	switch kind {
	case kindFloat:
		f := d.scanFloat()
		switch v := v.(type) {
		default:
			d.saveError(&DecodeTypeError{"float", v.Type()})
		case *reflect.FloatValue:
			if v.Overflow(f) {
				d.saveError(&DecodeTypeError{"float", v.Type()})
				break
			}
			v.Set(f)
		case *reflect.IntValue:
			n := int64(f)
			if v.Overflow(n) {
				d.saveError(&DecodeTypeError{"float", v.Type()})
				break
			}
			v.Set(n)
		case *reflect.InterfaceValue:
			v.Set(reflect.NewValue(f))
		}
	case kindString:
		s := d.scanString()
		switch v := v.(type) {
		default:
			d.saveError(&DecodeTypeError{"string", v.Type()})
		case *reflect.StringValue:
			v.Set(s)
		case *reflect.InterfaceValue:
			v.Set(reflect.NewValue(s))
		}
	case kindSymbol:
		s := d.scanString()
		switch v := v.(type) {
		default:
			d.saveError(&DecodeTypeError{"symbol", v.Type()})
		case *reflect.StringValue:
			v.Set(s)
		case *reflect.InterfaceValue:
			v.Set(reflect.NewValue(Symbol(s)))
		}
	case kindDocument:
		d.decodeDocument(v)
	case kindArray:
		d.error(os.NewError("array not supported yet"))
	case kindBinary:
		p, _ := d.scanBinary()
		switch v := v.(type) {
		default:
			d.saveError(&DecodeTypeError{"binary", v.Type()})
		case *reflect.InterfaceValue:
			newp := make([]byte, len(p))
			copy(newp, p)
			v.Set(reflect.NewValue(newp))
		case *reflect.SliceValue:
			t := v.Type().(*reflect.SliceType)
			if t.Elem().Kind() != reflect.Uint8 {
				d.saveError(&DecodeTypeError{"binary", v.Type()})
				break
			}
			if v.IsNil() || v.Len() < len(p) {
				v.Set(reflect.MakeSlice(t, len(p), len(p)))
			} else {
				v.SetLen(len(p))
			}
			reflect.ArrayCopy(v, reflect.NewValue(p).(reflect.ArrayOrSliceValue))
		}

	case kindObjectId:
		p := d.scanSlice(12)
		switch v := v.(type) {
		default:
			d.saveError(&DecodeTypeError{"objectid", v.Type()})
		case *reflect.ArrayValue:
			t := v.Type().(*reflect.ArrayType)
			if t.Elem().Kind() != reflect.Uint8 || t.Len() != 12 {
				d.saveError(&DecodeTypeError{"objectid", v.Type()})
				break
			}
			reflect.ArrayCopy(v, reflect.NewValue(p).(reflect.ArrayOrSliceValue))
		case *reflect.InterfaceValue:
			oid := ObjectId{}
			copy(oid[:], p)
			v.Set(reflect.NewValue(oid))
		}
	case kindBool:
		b := d.scanBool()
		switch v := v.(type) {
		default:
			d.saveError(&DecodeTypeError{"bool", v.Type()})
		case *reflect.BoolValue:
			v.Set(b)
		case *reflect.InterfaceValue:
			v.Set(reflect.NewValue(b))
		}
	case kindNull:
		switch v.(type) {
		default:
			d.saveError(&DecodeTypeError{"null", v.Type()})
		case *reflect.InterfaceValue, *reflect.PtrValue, *reflect.MapValue:
			v.SetValue(nil)
		}
	case kindInt32:
		n := d.scanInt32()
		switch v := v.(type) {
		default:
			d.error(&DecodeTypeError{"int32", v.Type()})
		case *reflect.IntValue:
			n := int64(n)
			if v.Overflow(n) {
				d.saveError(&DecodeTypeError{"int32", v.Type()})
				break
			}
			v.Set(n)
		case *reflect.InterfaceValue:
			v.Set(reflect.NewValue(n))
		case *reflect.FloatValue:
			v.Set(float64(n))
		}
	case kindInt64:
		n := d.scanInt64()
		switch v := v.(type) {
		default:
			d.error(&DecodeTypeError{"int64", v.Type()})
		case *reflect.IntValue:
			if v.Overflow(n) {
				d.saveError(&DecodeTypeError{"int64", v.Type()})
				break
			}
			v.Set(n)
		case *reflect.InterfaceValue:
			v.Set(reflect.NewValue(n))
		case *reflect.FloatValue:
			v.Set(float64(n))
		}
	case kindTimestamp:
		n := d.scanInt64()
		switch v := v.(type) {
		default:
			d.error(&DecodeTypeError{"timestamp", v.Type()})
		case *reflect.IntValue:
			if v.Overflow(n) {
				d.saveError(&DecodeTypeError{"timestamp", v.Type()})
				break
			}
			v.Set(n)
		case *reflect.InterfaceValue:
			v.Set(reflect.NewValue(Timestamp(n)))
		case *reflect.FloatValue:
			v.Set(float64(n))
		}
	case kindDateTime:
		n := d.scanInt64()
		switch v := v.(type) {
		default:
			d.error(&DecodeTypeError{"timestamp", v.Type()})
		case *reflect.IntValue:
			if v.Overflow(n) {
				d.saveError(&DecodeTypeError{"timestamp", v.Type()})
				break
			}
			v.Set(n)
		case *reflect.InterfaceValue:
			v.Set(reflect.NewValue(DateTime(n)))
		case *reflect.FloatValue:
			v.Set(float64(n))
		}
	case kindMinKey:
	case kindMaxKey:
	default:
		d.error(os.NewError("bson: unknown type"))
	}
}


func (d *decodeState) decodeValueInterface(kind int) interface{} {
	switch kind {
	case kindFloat:
		return d.scanFloat()
	case kindString:
		return d.scanString()
	case kindDocument:
		m := make(map[string]interface{})
		d.decodeDocumentInterface(m)
		return m
	case kindArray:
		d.error(os.NewError("array not supported yet"))
	case kindBinary:
		p, _ := d.scanBinary()
		newp := make([]byte, len(p))
		copy(newp, p)
		return p
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
		d.error(os.NewError("bson: unknown type"))
	}
	return nil
}

func (d *decodeState) skipValue(kind int) {
	switch kind {
	case kindFloat:
		d.offset += 8
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
	case kindDateTime, kindTimestamp, kindInt64:
		d.offset += 8
	case kindInt32:
		d.offset += 4
	case kindMinKey, kindMaxKey, kindNull:
		d.offset += 0
	default:
		d.error(os.NewError("bson: unknown type"))
	}
}

// indirect walks down v allocating pointers as needed, until it gets to a
// non-pointer.  If wantptr is true, indirect stops at the last pointer.
func (d *decodeState) indirect(v reflect.Value, wantptr bool) reflect.Value {
	for {
		if iv, ok := v.(*reflect.InterfaceValue); ok && !iv.IsNil() {
			v = iv.Elem()
			continue
		}
		pv, ok := v.(*reflect.PtrValue)
		if !ok {
			break
		}
		_, isptrptr := pv.Elem().(*reflect.PtrValue)
		if !isptrptr && wantptr {
			return pv
		}
		if pv.IsNil() {
			pv.PointTo(reflect.MakeZero(pv.Type().(*reflect.PtrType).Elem()))
		}
		v = pv.Elem()
	}
	return v
}
