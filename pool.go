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

type Pool struct {
	newFn func() (Conn, os.Error)
	conns chan Conn
}

type pooledConnection struct {
	Conn
	pool *Pool
}

func NewPool(newFn func() (Conn, os.Error), maxIdle int) *Pool {
	return &Pool{newFn: newFn, conns: make(chan Conn, maxIdle)}
}

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
