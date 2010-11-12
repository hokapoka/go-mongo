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

var (
	EOF = os.NewError("mongo: eof")
)

const (
	//If set, the database will insert the supplied object into the collection
	//if no matching document is found.
	UpdateUpsert = 1 << 0

	// If set, the database will insert the supplied object into the collection
	// if no matching document is found.
	UpdateMulti = 1 << 1

	//If set, the database will remove only the first matching document in the
	//collection. Otherwise all matching documents will be removed.
	RemoveSingle = 1 << 0
)

type FindOptions struct {
	// Optional document that limits the fields in the returned documents.
	// Fields contains one or more elements, each of which is the name of a
	// field that should be returned, and and the integer value 1. 
	Fields interface{}

	// Tailable means cursor is not closed when the last data is retrieved.
	// Rather, the cursor marks the final object's position. You can resume
	// using the cursor later, from where it was located, if more data were
	// received. 
	Tailable bool

	// Allow query of replica slave. Normally these return an error except for namespace "local".
	SlaveOk bool

	// The server normally times out idle cursors after an inactivity period
	// (10 minutes) to prevent excess memory use. Set this option to prevent
	// that.
	NoCursorTimeout bool

	// Use with TailableCursor. If we are at the end of the data, block for a
	// while rather than returning no data. After a timeout period, we do
	// return as normal.
	AwaitData bool

	// Stream the data down full blast in multiple "more" packages, on the
	// assumption that the client will fully read all data queried. Faster when
	// you are pulling a lot of data and know you want to pull it all down.
	// Note: the client is not allowed to not read all the data unless it
	// closes the connection.
	Exhaust bool

	// Sets the number of documents to omit - starting from the first document
	// in the resulting dataset - when returning the result of the query.
	Skip int

	Limit int
}

type Conn interface {
	Close() os.Error
	Update(namespace string, selector, update interface{}, options int) os.Error
	Insert(namespace string, documents ...interface{}) os.Error
	Remove(namespace string, selector interface{}, options int) os.Error
	FindOne(namespace string, query interface{}, options *FindOptions, result interface{}) os.Error
	Find(namespace string, query interface{}, options *FindOptions) (Cursor, os.Error)
}

type Cursor interface {
	Close() os.Error
	HasNext() bool
	Next(value interface{}) os.Error
}
