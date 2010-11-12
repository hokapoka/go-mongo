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
	"container/list"
	"sync"
)

type Pool struct {
	addr  string
	lock  sync.Mutex
	conns *list.List
}

type pooledConnection struct {
	*connection
	pool *Pool
}

func NewPool(addr string) *Pool {
	return &Pool{addr: addr, conns: list.New()}
}

func (p *Pool) get() interface{} {
	// To prevent connections from going stale, we take from the front and put
	// to the back of the list.
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.conns.Len() > 0 {
		return p.conns.Remove(p.conns.Front())
	}
	return nil
}

func (p *Pool) put(conn *connection) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.conns.PushBack(conn)
}

func (p *Pool) Get() (Conn, os.Error) {
	conn := p.get()
	if conn == nil {
		var err os.Error
		conn, err = Dial(p.addr)
		if err != nil {
			return nil, err
		}
	}
	return &pooledConnection{connection: conn.(*connection), pool: p}, nil
}

func (c *pooledConnection) Close() os.Error {
    if c.connection == nil {
        return nil
    }
	if c.connection.err == nil {
		c.pool.put(c.connection)
	}
	c.connection = nil
	return nil
}
