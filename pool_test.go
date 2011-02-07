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
	"testing"
	"os"
)

type fakeConn struct {
	klosed bool
	err    os.Error
}

func (c *fakeConn) Close() os.Error { c.klosed = true; return nil }
func (c *fakeConn) Error() os.Error { return c.err }
func (c *fakeConn) Update(namespace string, selector, update interface{}, options *UpdateOptions) os.Error {
	return nil
}
func (c *fakeConn) Insert(namespace string, documents ...interface{}) os.Error { return nil }
func (c *fakeConn) Remove(namespace string, selector interface{}, options *RemoveOptions) os.Error {
	return nil
}
func (c *fakeConn) Find(namespace string, query interface{}, options *FindOptions) (Cursor, os.Error) {
	return nil, nil
}

func TestPool(t *testing.T) {
	var count int
	p := NewPool(func() (Conn, os.Error) { count += 1; return &fakeConn{}, nil }, 2)

	count = 0
	for i := 0; i < 10; i++ {
		c1, _ := p.Get()
		c2, _ := p.Get()
		c1.Close()
		c2.Close()
	}
	if count != 2 {
		t.Fatal("expected count 1, actual", count)
	}

	p.Get()
	p.Get()
	count = 0
	for i := 0; i < 10; i++ {
		c, _ := p.Get()
		c.(*pooledConnection).Conn.(*fakeConn).err = os.EOF
		c.Close()
	}
	if count != 10 {
		t.Fatal("expected count 10, actual", count)
	}

	p.Get()
	p.Get()
	count = 0
	for i := 0; i < 10; i++ {
		c1, _ := p.Get()
		c2, _ := p.Get()
		c3, _ := p.Get()
		c1.Close()
		c2.Close()
		c3.Close()
	}
	if count != 12 {
		t.Fatal("expected count 12, actual", count)
	}
}
