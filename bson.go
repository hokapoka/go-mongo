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
	"encoding/binary"
	"reflect"
	"strings"
	"strconv"
)

var wire = binary.LittleEndian

type DateTime int64

type Timestamp int64

type CodeWithScope struct {
	Code  string
	Scope map[string]interface{}
}

type Regexp struct {
	Pattern string
	Flags   string
}

type ObjectId [12]byte

type Symbol string

type Code string

type KeyValue struct {
	Key   string
	Value interface{}
}

type OrderedMap []KeyValue

type Key int

const (
	MaxKey Key = 1
	MinKey Key = -1
)

var (
	typeDateTime           = reflect.Typeof(DateTime(0))
	typeTimestamp          = reflect.Typeof(Timestamp(0))
	typeCodeWithScope      = reflect.Typeof(CodeWithScope{})
	typeRegexp             = reflect.Typeof(Regexp{})
	typeObjectId           = reflect.Typeof(ObjectId{})
	typeOrderedMap         = reflect.Typeof(OrderedMap{})
	typeKey                = reflect.Typeof(MaxKey)
	typeCode               = reflect.Typeof(Code(""))
	typeSymbol             = reflect.Typeof(Symbol(""))
	typeString             = reflect.Typeof("")
	typeAny                = reflect.Typeof(interface{}(nil))
	typeByteSlice          = reflect.Typeof([]byte{})
	typeMapStringInterface = reflect.Typeof(make(map[string]interface{}))
)

const (
	kindFloat         = 0x1
	kindString        = 0x2
	kindDocument      = 0x3
	kindArray         = 0x4
	kindBinary        = 0x5
	kindObjectId      = 0x7
	kindBool          = 0x8
	kindDateTime      = 0x9
	kindNull          = 0xA
	kindRegexp        = 0xB
	kindCode          = 0xD
	kindSymbol        = 0xE
	kindCodeWithScope = 0xF
	kindInt32         = 0x10
	kindTimestamp     = 0x11
	kindInt64         = 0x12
	kindMinKey        = 0xff
	kindMaxKey        = 0x7f
)

var kindNames = map[int]string{
	kindFloat:         "float",
	kindString:        "string",
	kindDocument:      "document",
	kindArray:         "array",
	kindBinary:        "binary",
	kindObjectId:      "objectId",
	kindBool:          "bool",
	kindDateTime:      "dateTime",
	kindNull:          "null",
	kindRegexp:        "regexp",
	kindCode:          "code",
	kindSymbol:        "symbol",
	kindCodeWithScope: "codeWithScope",
	kindInt32:         "int32",
	kindTimestamp:     "timestamp",
	kindInt64:         "int64",
	kindMinKey:        "minKey",
	kindMaxKey:        "maxKey",
}

func kindName(kind int) string {
	name, ok := kindNames[kind]
	if !ok {
		name = strconv.Itoa(kind)
	}
	return name
}

func fieldName(f reflect.StructField) string {
	if f.Tag != "" {
		return f.Tag
	}
	if f.PkgPath == "" {
		return strings.ToLower(f.Name)
	}
	return ""
}
