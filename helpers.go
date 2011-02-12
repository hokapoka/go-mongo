// Copyright 2011 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mongo

import (
	"os"
	"strings"
)

// FindOne returns a single result for a query.
func FindOne(conn Conn, namespace string, query interface{}, options *FindOptions, result interface{}) os.Error {
	o := FindOptions{Limit: 1}
	if options != nil {
		o.Fields = options.Fields
		o.SlaveOk = options.SlaveOk
	}
	cursor, err := conn.Find(namespace, query, &o)
	if err != nil {
		return err
	}
	defer cursor.Close()
	return cursor.Next(result)
}

// RunCommand executes the command cmd on the database specified by the
// database component of namespace.
func RunCommand(conn Conn, namespace string, cmd, result interface{}) os.Error {
	if result == nil {
		result = &map[string]interface{}{}
	}
	db := namespace
	if i := strings.Index(namespace, "."); i > 0 {
		db = namespace[:i]
	}
	return FindOne(conn, db+".$cmd", cmd, nil, result)
}

var getLastErrorCmd = Doc{{"getLastError", 1}}

// GetLastError returns the last error for a database. The database is
// specified by the database component of namespace. The command cmd is used to
// fetch the last error. If cmd is nil, then the command {"getLasetError": 1}
// is used to get the error.  If the err argument is not nil, then err is
// returned directly from the function.
func GetLastError(conn Conn, namespace string, cmd interface{}, err os.Error) os.Error {
	if err != nil {
		return err
	}
	if cmd == nil {
		cmd = getLastErrorCmd
	}
	var r map[string]interface{}
	if err := RunCommand(conn, namespace, cmd, &r); err != nil {
		return err
	}
	if e := r["err"]; e != nil {
		return os.NewError(e.(string))
	}
	return nil
}

// SafeInsert returns the last error from the database after calling conn->Insert().
func SafeInsert(conn Conn, namespace string, errorCmd interface{}, documents ...interface{}) os.Error {
	return GetLastError(conn, namespace, errorCmd, conn.Insert(namespace, documents...))
}

// SafeUpdate returns the last error from the database after calling conn->Update().
func SafeUpdate(conn Conn, namespace string, errorCmd, selector, update interface{}, options *UpdateOptions) os.Error {
	return GetLastError(conn, namespace, errorCmd, conn.Update(namespace, selector, update, options))
}

// SafeRemove returns the last error from the database after calling conn->Remove().
func SafeRemove(conn Conn, namespace string, errorCmd, selector interface{}, options *RemoveOptions) os.Error {
	return GetLastError(conn, namespace, errorCmd, conn.Remove(namespace, selector, options))
}

// SafeConn wraps a connection with "safe" mode handling.
type SafeConn struct {
	// The connecion to wrap.
	Conn

	// The command document used to fetch the last error. If cmd is nil, then
	// the command {"getLastError": 1} is used as the command.
	Cmd interface{}
}

func (c SafeConn) Update(namespace string, selector, update interface{}, options *UpdateOptions) os.Error {
	return GetLastError(c.Conn, namespace, c.Cmd, c.Conn.Update(namespace, selector, update, options))
}

func (c SafeConn) Insert(namespace string, documents ...interface{}) os.Error {
	return GetLastError(c.Conn, namespace, c.Cmd, c.Conn.Insert(namespace, documents...))
}

func (c SafeConn) Remove(namespace string, selector interface{}, options *RemoveOptions) os.Error {
	return GetLastError(c.Conn, namespace, c.Cmd, c.Conn.Remove(namespace, selector, options))
}
