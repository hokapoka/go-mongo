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
	"os"
)

type Mongo interface {
	Db(name string) (Db, os.Error)
	DbNames() ([]string, os.Error)
}

type Db interface {
	Name() string
	CollectionNames() ([]string, os.Error)
	Collection(name string) (Collection, os.Error)            // collection must exist
	CreateCollection(name string) Collection                  // collection can exist
	CreateCollectionOptions(name string, options interface{}) // collection can exist
	DropCollection(name string) os.Error
	ExecuteCommand(command interface{}, document interface{}) os.Error
	Close() os.Error
	Drop()
	Admin() Admin
}

const (
	ProfilingLevelOff      = 0
	ProfilingLevelSlowOnly = 1
	ProfilingLevelSlowAll  = 2
)

type Admin interface {
	// ProfilingLevel returns one of the ProfilingLevel* values.
	ProfilingLevel() (int, os.Error)
	SetProfilingLevel(int) os.Error
	ProfilingInfo() (map[string]interface{}, os.Error)
	ValidateCollection(name string) bool
}

type Collection interface {
	Find(doc interface{}) (Cursor, os.Error)
	Insert(doc interface{}) os.Error
	Remove(query interface{}) os.Error
	Modify(selector, modifier interface{}) os.Error
	Replace(selector, object interface{}) os.Error
	Repsert(selector, object interface{}) os.Error
	Count() int64
	CountQuery(doc interface{}) int64
}

type Cursor interface{}
