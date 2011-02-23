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


// EnsureIndex creates an index on the collection based on 
// the values in keys.  If the index allready exists it
// returns an error.
//   - keys map[string]int defines the properties for the 
//     collection  to be indexed.
func EnsureIndex(conn Conn, namespace string, keys map[string]int, unique, sparse bool) os.Error {

    if len(keys) == 0 {
        return os.NewError("You need to define some keys to index on")
    }
   
    i := strings.Index(namespace, ".")
    if i <= 0 {
        return os.NewError("You need to define a full namespace to declare an index on")
    }
    idxns := namespace[:i] + ".system.indexes"

    // Might be better to use fmt.Sprintf for this, but inc. the "fmt" package
    // makes the linked package very large b/c of the unicode stuff
    name := ""
    for k, v := range keys {  
        if len(name) > 0 {
            name += "_"
        }  
        name += k + "_" + strconv.Itoa(v)
    }
   
    var curidx map[string]interface{}
    err := FindOne(conn, idxns, map[string]interface{}{ "name":name, "ns":namespace }, nil, &curidx)
    if err == nil {
        return os.NewError("Index allready exists")
    }
   
    idx := map[string]interface{}{
        "ns":namespace,
        "key":keys,
        "name":name,
    }
   
    if unique {
        idx["unique"] = true
    }
   
    if sparse {
        idx["sparse"] = true
    }
   
    err = conn.Insert( idxns, idx )
    if err != nil {
        return err
    }
   
    return nil

}

// Count issues a RunCommand over the given connection issuing the 
// count command that uses more efficient server-side counting of the
// documents that match the specified query
func Count(conn Conn, namespace string, query interface{}) (float64, os.Error) {

    coll := namespace
   
    i := strings.Index(namespace, ".")
    if i <= 0 || i == len(namespace) -1 {
        return -1, os.NewError("Invalid namespace")
    }
    coll = namespace[i+1:]
   
    cmd := Doc{
        {"count", coll},
        {"query", query},
    }
   
    r, err := RunCommand( conn, namespace, cmd)
    if err != nil {
        return -1, err
    }
    n := (map[string]interface{})(r)["n"].(float64)
   
    return n, nil

}

