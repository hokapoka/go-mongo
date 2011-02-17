// Copyright 2011 Gary Burd
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
	"log"
	"sync"
)

var (
	logIdMutex sync.Mutex
	logId      int
)

func newLogId() int {
	logIdMutex.Lock()
	defer logIdMutex.Unlock()
	logId += 1
	return logId
}

// NewLoggerConn returns logging wrapper around a connection.
func NewLoggerConn(conn Conn) Conn {
	return logConn{conn, newLogId()}
}

type logConn struct {
	Conn
	id int
}

func (c logConn) Close() os.Error {
	err := c.Conn.Close()
	log.Printf("%d.Close() (err: %v)", c.id, err)
	return err
}

func (c logConn) Update(namespace string, selector, update interface{}, options *UpdateOptions) os.Error {
	err := c.Conn.Update(namespace, selector, update, options)
	log.Printf("%d.Update(namespace, %+v, %+v, %+v) (%v)", c.id, namespace, selector, update, options, err)
	return err
}

func (c logConn) Insert(namespace string, documents ...interface{}) os.Error {
	err := c.Conn.Insert(namespace, documents...)
	log.Printf("%d.Insert(%s, %+v) (%v)", c.id, namespace, documents, err)
	return err
}

func (c logConn) Remove(namespace string, selector interface{}, options *RemoveOptions) os.Error {
	err := c.Conn.Remove(namespace, selector, options)
	log.Printf("%d.Remove(%s, %+v, %+v) (%v)", c.id, namespace, selector, options, err)
	return err
}

func (c logConn) Find(namespace string, query interface{}, options *FindOptions) (Cursor, os.Error) {
	r, err := c.Conn.Find(namespace, query, options)
	var id int
	if r != nil {
		id = newLogId()
		r = logCursor{r, id}
	}
	log.Printf("%d.Find(%s, %+v, %+v) (%d, %v)", c.id, namespace, query, options, id, err)
	return r, err
}

type logCursor struct {
	Cursor
	id int
}

func (r logCursor) Close() os.Error {
	err := r.Cursor.Close()
	log.Printf("%d.Close() (%v)", r.id, err)
	return err
}

func (r logCursor) Next(value interface{}) os.Error {
	var bd BSONData
	err := r.Cursor.Next(&bd)
	var m map[string]interface{}
	if err == nil {
		err = Decode(bd.Data, value)
		Decode(bd.Data, &m)
	}
	log.Printf("%d.Next() (%v, %v)", r.id, m, err)
	return err
}
