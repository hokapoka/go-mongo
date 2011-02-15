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

// Pool maintains a pool of database connections.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers, possibly using a global variable:
//
//  pool = mongo.NewDialPool("localhost", 2)
//
// This pool uses a simple database connection and a maximum of two idle
// connections.
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  conn, err := pool.Get()
//  if err != nil {
//      // handle the error
//  }
//  defer conn.Close()
//  // do something with the connection
//
// Close() returns the connection to the pool if there's room in the pool and
// the connection does not have a permanent error. Otherwise, Close() releases
// the resources used by the connection.
type Pool struct {
	newFn func() (Conn, os.Error)
	conns chan Conn
}

type pooledConnection struct {
	Conn
	pool *Pool
}

// NewDialPool returns a new connetion pool. The pool uses mongo.Dial to
// create new connections and maintains a maximum of maxIdle connetions.
func NewDialPool(addr string, maxIdle int) *Pool {
	return NewPool(func() (Conn, os.Error) { return Dial(addr) }, maxIdle)
}

// NewPool returns a new connection pool. The pool uses newFn to create
// connections as needed and maintains a maximum of maxIdle idle connections.
func NewPool(newFn func() (Conn, os.Error), maxIdle int) *Pool {
	return &Pool{newFn: newFn, conns: make(chan Conn, maxIdle)}
}

// Get returns an idle connection from the pool if available or creates a new
// connection. The caller should Close() the connection to return the
// connection to the pool.
func (p *Pool) Get() (Conn, os.Error) {
	var c Conn
	select {
	case c = <-p.conns:
	default:
		var err os.Error
		c, err = p.newFn()
		if err != nil {
			return nil, err
		}
	}
	return &pooledConnection{Conn: c, pool: p}, nil
}

func (c *pooledConnection) Close() os.Error {
	if c.Conn == nil {
		return nil
	}
	if c.Error() != nil {
		return nil
	}
	select {
	case c.pool.conns <- c.Conn:
	default:
		c.Conn.Close()
	}
	c.Conn = nil
	return nil
}
