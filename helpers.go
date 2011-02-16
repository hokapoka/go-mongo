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

// CommandNamespace returns the command namespace name.$cmd given a database
// name or a collection namespace.
func CommandNamespace(nameOrNamespace string) string {
	s := nameOrNamespace
	if i := strings.Index(nameOrNamespace, "."); i > 0 {
		s = nameOrNamespace[:i]
	}
	return s + ".$cmd"
}

// RunCommand executes the command cmd on the database specified by the
// database component of namespace. If cmd is a string, then the command {cmd:
// 1} is sent to the database. 
func RunCommand(conn Conn, namespace string, cmd interface{}) (map[string]interface{}, os.Error) {
	if s, ok := cmd.(string); ok {
		cmd = Doc{{s, 1}}
	}
	result := map[string]interface{}{}
	if err := FindOne(conn, CommandNamespace(namespace), cmd, nil, &result); err != nil {
		return nil, err
	}
	if errmsg, ok := result["errmsg"].(string); ok {
		return result, os.NewError(errmsg)
	}
	return result, nil
}

// GetLastError returns the last error for a database. The database is
// specified by the database component of namespace. The command cmd is used to
// fetch the last error. If cmd is nil, then the command {"getLasetError": 1}
// is used to get the error. 
func GetLastError(conn Conn, namespace string, cmd interface{}) os.Error {
	if cmd == nil {
		cmd = "getLastError"
	}
	result, err := RunCommand(conn, namespace, cmd)
	if err != nil {
		return err
	}
	if s, ok := result["err"].(string); ok && s != "" {
		return os.NewError(s)
	}
	return nil
}

// SafeInsert returns the last error from the database after calling conn.Insert().
func SafeInsert(conn Conn, namespace string, errorCmd interface{}, documents ...interface{}) os.Error {
	if err := conn.Insert(namespace, documents...); err != nil {
		return err
	}
	return GetLastError(conn, namespace, errorCmd)
}

// SafeUpdate returns the last error from the database after calling conn.Update().
func SafeUpdate(conn Conn, namespace string, errorCmd, selector, update interface{}, options *UpdateOptions) os.Error {
	if err := conn.Update(namespace, selector, update, options); err != nil {
		return err
	}
	return GetLastError(conn, namespace, errorCmd)
}

// SafeRemove returns the last error from the database after calling conn.Remove().
func SafeRemove(conn Conn, namespace string, errorCmd, selector interface{}, options *RemoveOptions) os.Error {
	if err := conn.Remove(namespace, selector, options); err != nil {
		return err
	}
	return GetLastError(conn, namespace, errorCmd)
}

// SafeConn wraps a connection with safe mode handling. The wrapper fetches the
// last error from the server after each call to a mutating operation (insert,
// update, remove) and returns the error if any as an os.Error.
type SafeConn struct {
	// The connecion to wrap.
	Conn

	// The command document used to fetch the last error. If cmd is nil, then
	// the command {"getLastError": 1} is used as the command.
	Cmd interface{}
}

func (c SafeConn) Update(namespace string, selector, update interface{}, options *UpdateOptions) os.Error {
	return SafeUpdate(c.Conn, namespace, c.Cmd, selector, update, options)
}

func (c SafeConn) Insert(namespace string, documents ...interface{}) os.Error {
	return SafeInsert(c.Conn, namespace, c.Cmd, documents...)
}

func (c SafeConn) Remove(namespace string, selector interface{}, options *RemoveOptions) os.Error {
	return SafeRemove(c.Conn, namespace, c.Cmd, selector, options)
}
