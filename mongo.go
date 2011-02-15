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

// The mongo package is a driver for MongoDB. 
//
// Go-Mongo uses a namespace string to specify the database and collection for
// all operations. The namespace string is in the format
// "<database>.<collection>" where <database> is the name of the database and
// <collection> is the name of the collection. 
package mongo

import (
	"os"
)

var (
	// No more data in cursor.
	EOF = os.NewError("mongo: eof")
)

// RemoveOptions specifies options for the Conn.Remove method.
type RemoveOptions struct {
	// If set, the database will remove only the first matching document in the
	// collection. Otherwise all matching documents will be removed.
	Single bool
}

// UpdateOptions specifies options for the Conn.Update method.
type UpdateOptions struct {
	// If set, the database will insert the supplied object into the collection
	// if no matching document is found.
	Upsert bool

	// If set, the database will update all objects matching the query.
	Multi bool
}

// FindOptions specifies options for the Conn.Find method.
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

	// Allow query of replica slave. Normally these return an error except for
	// namespace "local".
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

	// Sets the number of documents to return. 
	Limit int

	// Sets the batch size used for sending documents from the server to the
	// client.
	BatchSize int
}

// A Conn represents a connection to a MongoDB server. 
//
// When the application is done using the connection, the application must call
// the connection Close() method to release the resources used by the
// connection. 
type Conn interface {
	// Close releases the resources used by this connection.
	Close() os.Error

	// Error returns non-nil if the connection has a permanent error.
	Error() os.Error

	// Update document specified by selector with update.
	Update(namespace string, selector, update interface{}, options *UpdateOptions) os.Error

	// Insert documents.
	Insert(namespace string, documents ...interface{}) os.Error

	// Remove documents specified by seletor.
	Remove(namespace string, selector interface{}, options *RemoveOptions) os.Error

	// Find documents specified by selector. The returned cursor must be closed.
	Find(namespace string, query interface{}, options *FindOptions) (Cursor, os.Error)
}

// Cursor iterates over the results from a Find operation.
//
// When the application is done using a cursor, the application must call the
// cursor Close() method to release the resources used by the cursor.
//
// An example use of a cursor is:
//
//  cursor, err := c.Find("db.coll", mongo.Doc{}, nil)
//  if err != nil {
//      return err
//  }
//  defer cursor.Close()
//
//  for cursor.HasNext() {
//      var m map[string]interface{}
//      err = r.Next(&m)
//      if err != nil {
//          return err
//      }
//      // Do something with result document m.
//	}
//
// Tailable cursors are supported. When working with a tailable cursor, use the
// expression cursor.Error() != nil to determine if the cursor is "dead." See
// http://www.mongodb.org/display/DOCS/Tailable+Cursors for more discussion on
// tailable cursors.
type Cursor interface {
	// Close releases the resources used by this connection. 
	Close() os.Error

	// Error returns non-nil if the cursor has a permanent error. 
	Error() os.Error

	// HasNext returns true if there are more documents to retrieve.
	HasNext() bool

	// Next fetches the next document from the cursor.
	Next(value interface{}) os.Error
}
