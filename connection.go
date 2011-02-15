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
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

const (
	updateUpsert         = 1 << 0
	updateMulti          = 1 << 1
	removeSingle         = 1 << 0
	queryTailable        = 1 << 1
	querySlaveOk         = 1 << 2
	queryNoCursorTimeout = 1 << 4
	queryAwaitData       = 1 << 5
	queryExhaust         = 1 << 6
)

type connection struct {
	conn      net.Conn
	addr      string
	requestId uint32
	cursors   map[uint32]*cursor
	err       os.Error
}

// Dial connects to server at addr.
func Dial(addr string) (Conn, os.Error) {
	if strings.LastIndex(addr, ":") <= strings.LastIndex(addr, "]") {
		addr = addr + ":27017"
	}
	c := connection{
		addr:    addr,
		cursors: make(map[uint32]*cursor),
	}
	return &c, c.connect()
}

func (c *connection) connect() os.Error {
	conn, err := net.Dial("tcp", "", c.addr)
	if err != nil {
		return err
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = conn
	return nil
}

func (c *connection) nextId() uint32 {
	c.requestId += 1
	return c.requestId
}

func (c *connection) fatal(err os.Error) os.Error {
	if c.err == nil {
		c.Close()
		c.err = err
	}
	return err
}

// Close closes the connection to the server.
func (c *connection) Close() os.Error {
	var err os.Error
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
		c.cursors = nil
		c.err = os.NewError("mongo: connection closed")
	}
	return err
}

func (c *connection) Error() os.Error {
	return c.err
}

// send sets the messages length and writes the message to the socket.
func (c *connection) send(msg []byte) os.Error {
	if c.err != nil {
		return c.err
	}
	wire.PutUint32(msg[0:4], uint32(len(msg)))
	_, err := c.conn.Write(msg)
	if err != nil {
		return c.fatal(err)
	}
	return nil
}

func (c *connection) Update(namespace string, document, selector interface{}, options *UpdateOptions) (err os.Error) {

	flags := 0
	if options != nil {
		if options.Upsert {
			flags |= updateUpsert
		}
		if options.Multi {
			flags |= updateMulti
		}
	}

	b := buffer(make([]byte, 0, 512))
	b.Next(4)                    // placeholder for message length
	b.WriteUint32(c.nextId())    // requestId
	b.WriteUint32(0)             // responseTo
	b.WriteUint32(2001)          // opCode
	b.WriteUint32(0)             // reserved
	b.WriteString(namespace)     // namespace
	b.WriteByte(0)               // null terminator
	b.WriteUint32(uint32(flags)) // flags
	b, err = Encode(b, document)
	if err != nil {
		return err
	}
	b, err = Encode(b, selector)
	if err != nil {
		return err
	}
	return c.send(b)
}

func (c *connection) Insert(namespace string, documents ...interface{}) (err os.Error) {
	if len(documents) == 0 {
		return os.NewError("mongo: insert with no documents")
	}
	b := buffer(make([]byte, 0, 512))
	b.Next(4)                 // placeholder for message length
	b.WriteUint32(c.nextId()) // requestId
	b.WriteUint32(0)          // responseTo
	b.WriteUint32(2002)       // opCode
	b.WriteUint32(0)          // reserved
	b.WriteString(namespace)  // namespace
	b.WriteByte(0)            // null terminator
	for _, document := range documents {
		b, err = Encode(b, document)
		if err != nil {
			return err
		}
	}
	return c.send(b)
}

func (c *connection) Remove(namespace string, selector interface{}, options *RemoveOptions) (err os.Error) {

	flags := 0
	if options != nil {
		if options.Single {
			flags |= removeSingle
		}
	}
	b := buffer(make([]byte, 0, 512))
	b.Next(4)                    // placeholder for message length
	b.WriteUint32(c.nextId())    // requestId
	b.WriteUint32(0)             // responseTo
	b.WriteUint32(2006)          // opCode
	b.WriteUint32(0)             // reserved
	b.WriteString(namespace)     // namespace
	b.WriteByte(0)               // null terminator
	b.WriteUint32(uint32(flags)) // flags
	b, err = Encode(b, selector)
	if err != nil {
		return err
	}
	return c.send(b)
}

func (c *connection) Find(namespace string, query interface{}, options *FindOptions) (Cursor, os.Error) {

	r := cursor{
		conn:      c,
		namespace: namespace,
		requestId: c.nextId(),
	}

	var fields interface{}
	var skip int
	if options != nil {
		skip = options.Skip
		fields = options.Fields
		r.limit = options.Limit
		r.batchSize = options.BatchSize
		if r.batchSize == 1 {
			// Server handles numberToReturn == 1 as hard limit. Change value
			// to two to avoid having batch size set hard limit.
			r.batchSize = 2
		}
		if options.Tailable {
			r.flags |= queryTailable
			r.limit = 0
		}
		if options.SlaveOk {
			r.flags |= querySlaveOk
		}
		if options.NoCursorTimeout {
			r.flags |= queryNoCursorTimeout
		}
		if options.AwaitData {
			r.flags |= queryAwaitData
		}
		if options.Exhaust {
			r.flags |= queryExhaust
		}
	}

	b := buffer(make([]byte, 0, 512))
	b.Next(4)                         // placeholder for message length
	b.WriteUint32(r.requestId)        // requestId
	b.WriteUint32(0)                  // responseTo
	b.WriteUint32(2004)               // opCode
	b.WriteUint32(uint32(r.flags))    // flags
	b.WriteString(namespace)          // namespace
	b.WriteByte(0)                    // null terminator
	b.WriteUint32(uint32(skip))       // numberToSkip
	b.WriteUint32(r.numberToReturn()) // numberToReturn
	b, err := Encode(b, query)
	if err != nil {
		return nil, err
	}
	if fields != nil {
		b, err = Encode(b, fields)
		if err != nil {
			return nil, err
		}
	}
	err = c.send(b)
	if err != nil {
		return nil, err
	}

	c.cursors[r.requestId] = &r
	return &r, nil
}

func (c *connection) getMore(r *cursor) os.Error {
	r.requestId = c.nextId()
	b := buffer(make([]byte, 0, 5*4+len(r.namespace)+1+4+8))
	b.Next(4)                  // placeholder for message length
	b.WriteUint32(r.requestId) // requestId
	b.WriteUint32(0)           // responseTo
	b.WriteUint32(2005)        // opCode
	b.WriteUint32(0)           // reserved
	b.WriteString(r.namespace) // namespace
	b.WriteByte(0)             // null terminator
	b.WriteUint32(r.numberToReturn())
	b.WriteUint64(r.cursorId)
	if err := c.send(b); err != nil {
		return err
	}
	c.cursors[r.requestId] = r
	return nil
}

func (c *connection) killCursors(cursorIds ...uint64) os.Error {
	b := buffer(make([]byte, 0, 6*4+len(cursorIds)*8))
	b.Next(4)                             // placeholder for message length
	b.WriteUint32(c.nextId())             // requestId
	b.WriteUint32(0)                      // responseTo
	b.WriteUint32(2007)                   // opCode
	b.WriteUint32(0)                      // zero
	b.WriteUint32(uint32(len(cursorIds))) // number of cursor ids.
	for _, cursorId := range cursorIds {
		b.WriteUint64(cursorId)
	}
	return c.send(b)
}

type response struct {
	flags uint32
	count int
	data  []byte
}

type cursor struct {
	conn      *connection
	namespace string
	requestId uint32
	cursorId  uint64
	limit     int
	batchSize int
	count     int
	resp      []response
	flags     int
	err       os.Error
}

// receive recieves a single response from the server and delivers it to the appropriate cursor.
func (c *connection) receive() os.Error {
	if c.err != nil {
		return c.err
	}

	var buf [36]byte
	if _, err := io.ReadFull(c.conn, buf[:]); err != nil {
		return c.fatal(err)
	}

	messageLength := int32(wire.Uint32(buf[0:4]))
	requestId := wire.Uint32(buf[4:8])
	responseTo := wire.Uint32(buf[8:12])
	opCode := int32(wire.Uint32(buf[12:16]))
	flags := wire.Uint32(buf[16:20])
	cursorId := wire.Uint64(buf[20:28])
	//startingFrom := int32(wire.Uint32(buf[28:32]))
	count := int(wire.Uint32(buf[32:36]))
	data := make([]byte, messageLength-36)

	if _, err := io.ReadFull(c.conn, data); err != nil {
		return c.fatal(err)
	}

	if opCode != 1 {
		return c.fatal(os.NewError(fmt.Sprintf("mongo: unknown response opcode %d", opCode)))
	}

	r, found := c.cursors[responseTo]
	if !found {
		if cursorId != 0 {
			c.killCursors(cursorId)
		}
		return nil
	}

	c.cursors[responseTo] = nil, false
	r.requestId = 0
	r.cursorId = cursorId
	if r.flags&queryExhaust != 0 && cursorId != 0 {
		r.requestId = requestId
		c.cursors[requestId] = r
	}

	r.resp = append(r.resp, response{flags: flags, count: count, data: data})
	return nil
}

func (r *cursor) numberToReturn() uint32 {
	n := r.batchSize
	if r.limit > 0 {
		n = r.limit - r.count
		if r.batchSize > 0 && r.batchSize < n {
			n = r.batchSize
		}
	}
	return uint32(n)
}

func (r *cursor) fatal(err os.Error) os.Error {
	if r.err == nil {
		r.Close()
		r.err = err
	}
	return err
}

func (r *cursor) Close() os.Error {
	if r.err != nil {
		return nil
	}
	if r.requestId != 0 {
		r.conn.cursors[r.requestId] = nil, false
	}
	if r.cursorId != 0 {
		r.conn.killCursors(r.cursorId)
	}
	r.err = os.NewError("mongo: cursor closed")
	r.conn = nil
	r.resp = nil
	return nil
}

func (r *cursor) fill() os.Error {
	if r.err != nil {
		return r.err
	}

	if r.limit > 0 && r.count >= r.limit {
		return r.fatal(EOF)
	}

	if len(r.resp) > 0 {
		if r.resp[0].count > 0 {
			return nil
		}
		r.resp = r.resp[1:]
	}

	if len(r.resp) == 0 {
		if r.requestId == 0 {
			if r.cursorId == 0 {
				return r.fatal(EOF)
			}
			if r.flags&queryExhaust == 0 {
				var err os.Error
				err = r.conn.getMore(r)
				if err != nil {
					return r.fatal(err)
				}
			}
		}
		for len(r.resp) == 0 {
			err := r.conn.receive()
			if err != nil {
				return r.fatal(err)
			}
		}
	}

	const (
		cursorNotFound = 1 << 0
		queryFailure   = 1 << 1
	)

	if r.resp[0].flags&cursorNotFound != 0 {
		return r.fatal(os.NewError("mongo: cursor not found"))
	}

	if r.resp[0].flags&queryFailure != 0 {
		var m map[string]interface{}
		err := Decode(r.resp[0].data, &m)
		if err != nil {
			return r.fatal(err)
		} else if s, ok := m["$err"].(string); ok {
			return r.fatal(os.NewError(s))
		} else {
			return r.fatal(os.NewError("mongo: query failure"))
		}
	}

	if r.resp[0].count == 0 {
		r.resp = r.resp[1:]
	}

	if len(r.resp) == 0 && (r.cursorId == 0 || (r.flags&queryTailable) == 0) {
		return r.fatal(EOF)
	}

	return nil
}

func (r *cursor) Error() os.Error {
	return r.err
}

func (r *cursor) HasNext() bool {
	if err := r.fill(); err != nil {
		return err != EOF
	}
	return len(r.resp) > 0
}

func (r *cursor) Next(value interface{}) os.Error {
	if err := r.fill(); err != nil {
		return err
	}
	if len(r.resp) == 0 {
		// tailable, no data available now
		return EOF
	}
	if len(r.resp[0].data) < 4 {
		fmt.Println("DATA", len(r.resp[0].data), r.resp[0].count)
		return r.fatal(os.NewError("mongo: response data corrupted"))
	}
	n := int(wire.Uint32(r.resp[0].data[0:4]))
	if n > len(r.resp[0].data) {
		return r.fatal(os.NewError("mongo: response data corrupted"))
	}
	err := Decode(r.resp[0].data[0:n], value)
	r.resp[0].data = r.resp[0].data[n:]
	r.resp[0].count -= 1
	r.count += 1
	return err
}
