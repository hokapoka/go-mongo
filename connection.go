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
	"net"
	"os"
	"strings"
	"io"
	"log"
)

type response struct {
	requestId uint32
	cursorId  uint64
	count     int
	data      []byte
	err       os.Error
}

type connection struct {
	conn      *net.TCPConn
	addr      string
	requestId uint32
	responses map[uint32]interface{}
	err       os.Error
}

// Dial connects to server at addr.
func Dial(addr string) (Conn, os.Error) {
	if strings.LastIndex(addr, ":") <= strings.LastIndex(addr, "]") {
		addr = addr + ":27017"
	}
	c := connection{addr: addr, responses: make(map[uint32]interface{})}
	return &c, c.reconnect()
}

func (c *connection) reconnect() os.Error {
	conn, err := net.Dial("tcp", "", c.addr)
	if err != nil {
		return err
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = conn.(*net.TCPConn)
	return nil
}

func (c *connection) nextId() uint32 {
	c.requestId += 1
	return c.requestId
}

func (c *connection) fatal(err os.Error) os.Error {
	log.Println("connection faital", err)
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
		c.responses = nil
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

// receive recieves a single response from the server.
func (c *connection) receive() (*response, os.Error) {
	if c.err != nil {
		return nil, c.err
	}

	var buf [36]byte
	if _, err := io.ReadFull(c.conn, buf[:]); err != nil {
		return nil, c.fatal(err)
	}

	var r response
	messageLength := int32(wire.Uint32(buf[0:4]))  // messageLength
	requestId := wire.Uint32(buf[4:8])             // requestId
	r.requestId = wire.Uint32(buf[8:12])           // responseTo 
	opCode := int32(wire.Uint32(buf[12:16]))       // opCode
	flags := int(wire.Uint32(buf[16:20]))          // responseFlags
	r.cursorId = wire.Uint64(buf[20:28])           // cursorId
	startingFrom := int32(wire.Uint32(buf[28:32])) // startingFrom
	r.count = int(wire.Uint32(buf[32:36]))         // numberReturned
	r.data = make([]byte, messageLength-36)

	log.Println("RESPONSE",
		"len:", messageLength,
		"reqId:", requestId,
		"respId:", r.requestId,
		"flags:", flags,
		"cursor:", r.cursorId,
		"start:", startingFrom,
		"count:", r.count)

	if _, err := io.ReadFull(c.conn, r.data); err != nil {
		return nil, c.fatal(err)
	}
	if opCode != 1 {
		return nil, c.fatal(os.NewError(fmt.Sprintf("mongo: unknown response opcode %d", opCode)))
	}

	const (
		cursorNotFound = 1 << 0
		queryFailure   = 1 << 1
	)

	if flags&cursorNotFound != 0 {
		r.err = os.NewError("mongo: cursor not found")
	} else if flags&queryFailure != 0 {
		var m map[string]interface{}
		err := Decode(r.data, &m)
		if err != nil {
			r.err = os.NewError("mongo: query failure")
		} else if s, ok := m["$err"].(string); ok {
			r.err = os.NewError(s)
		} else {
			r.err = os.NewError("mongo: query failure")
		}
	}
	return &r, nil
}

// wait waits for a response to the given request. Reponses to other requests
// are saved. 
func (c *connection) wait(requestId uint32) (*response, os.Error) {
	if c.err != nil {
		return nil, c.err
	}

	r := c.responses[requestId]
	c.responses[requestId] = nil, false

	switch r := r.(type) {
	case *response:
		return r, r.err
	case nil:
		return nil, os.NewError("mongo: not expecting response")
	default:
		for {
			r, err := c.receive()
			if err != nil {
				return nil, err
			}
			if r.requestId == requestId {
				return r, r.err
			} else if _, found := c.responses[r.requestId]; found {
				c.responses[r.requestId] = r
			} else if r.cursorId != 0 {
				c.killCursors(r.cursorId)
			}
		}
	}
	return nil, os.NewError("mongo: unexpected")
}

func (c *connection) getMore(namespace string, numberToReturn int, cursorId uint64) (uint32, os.Error) {
	requestId := c.nextId()
	b := buffer(make([]byte, 0, 5*4+len(namespace)+1+4+8))
	b.Next(4)                // placeholder for message length
	b.WriteUint32(requestId) // requestId
	b.WriteUint32(0)         // responseTo
	b.WriteUint32(2005)      // opCode
	b.WriteUint32(0)         // reserved
	b.WriteString(namespace) // namespace
	b.WriteByte(0)           // null terminator
	b.WriteUint32(uint32(numberToReturn))
	b.WriteUint64(cursorId)
	err := c.send(b)
	if err != nil {
		return 0, err
	}
	c.responses[requestId] = true
	return requestId, nil
}

const (
	queryTailable        = 1 << 1
	querySlaveOk         = 1 << 2
	queryNoCursorTimeout = 1 << 4
	queryAwaitData       = 1 << 5
	queryExhaust         = 1 << 6
)

func (c *connection) query(namespace string, query, returnFieldSelector interface{}, skip, count, flags int) (uint32, os.Error) {
	requestId := c.nextId()
	b := buffer(make([]byte, 0, 512))
	b.Next(4)                    // placeholder for message length
	b.WriteUint32(requestId)     // requestId
	b.WriteUint32(0)             // responseTo
	b.WriteUint32(2004)          // opCode
	b.WriteUint32(uint32(flags)) // flags
	b.WriteString(namespace)     // namespace
	b.WriteByte(0)               // null terminator
	b.WriteUint32(uint32(skip))  // numberToSkip
	b.WriteUint32(uint32(count)) // numberToReturn
	b, err := Encode(b, query)
	if err != nil {
		return 0, err
	}
	if returnFieldSelector != nil {
		b, err = Encode(b, returnFieldSelector)
		if err != nil {
			return 0, err
		}
	}
	err = c.send(b)
	if err != nil {
		return 0, err
	}
	c.responses[requestId] = true
	return requestId, nil
}

func (c *connection) killCursors(cursorIds ...uint64) os.Error {
	b := buffer(make([]byte, 5*4*len(cursorIds)*8))
	b.Next(4)                 // placeholder for message length
	b.WriteUint32(c.nextId()) // requestId
	b.WriteUint32(0)          // responseTo
	b.WriteUint32(2007)       // opCode
	b.WriteUint32(0)          // reserved
	for _, cursorId := range cursorIds {
		b.WriteUint64(cursorId)
	}
	return c.send(b)
}

func (c *connection) Update(namespace string, document, selector interface{}, options UpdateOption) (err os.Error) {
	b := buffer(make([]byte, 0, 512))
	b.Next(4)                      // placeholder for message length
	b.WriteUint32(c.nextId())      // requestId
	b.WriteUint32(2001)            // opCode
	b.WriteUint32(0)               // reserved
	b.WriteString(namespace)       // namespace
	b.WriteByte(0)                 // null terminator
	b.WriteUint32(uint32(options)) // flags
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

func (c *connection) Remove(namespace string, selector interface{}, options RemoveOption) (err os.Error) {
	b := buffer(make([]byte, 0, 512))
	b.Next(4)                      // placeholder for message length
	b.WriteUint32(c.nextId())      // requestId
	b.WriteUint32(0)               // responseTo
	b.WriteUint32(2006)            // opCode
	b.WriteUint32(0)               // reserved
	b.WriteString(namespace)       // namespace
	b.WriteByte(0)                 // null terminator
	b.WriteUint32(uint32(options)) // flags
	b, err = Encode(b, selector)
	if err != nil {
		return err
	}
	return c.send(b)
}

func (c *connection) FindOne(namespace string, query interface{}, options *FindOptions, result interface{}) os.Error {
	var fields interface{}
	if options != nil {
		fields = options.Fields
	}
	requestId, err := c.query(namespace, query, fields, 0, 1, 0)
	if err != nil {
		return err
	}
	r, err := c.wait(requestId)
	if err != nil {
		return err
	}
	if r.cursorId != 0 {
		c.killCursors(r.cursorId)
	}
	if r.count < 1 {
		return EOF
	}
	return Decode(r.data, result)
}

func (c *connection) Find(namespace string, query interface{}, options *FindOptions) (Cursor, os.Error) {
	var fields interface{}
	var skip, count, flags int
	if options != nil {
		skip = options.Skip
		fields = options.Fields
		if options.Tailable {
			flags |= queryTailable
		}
		if options.SlaveOk {
			flags |= querySlaveOk
		}
		if options.NoCursorTimeout {
			flags |= queryNoCursorTimeout
		}
		if options.AwaitData {
			flags |= queryAwaitData
		}
		if options.Exhaust {
			flags |= queryExhaust
		}
	}
	requestId, err := c.query(namespace, query, fields, skip, count, flags)
	if err != nil {
		return nil, err
	}
	return &cursor{conn: c, namespace: namespace, requestId: requestId}, nil
}

type cursor struct {
	conn      *connection
	namespace string
	requestId uint32
	cursorId  uint64
	count     int
	data      []byte
	err       os.Error
}

func (c *cursor) fatal(err os.Error) os.Error {
	if c.err == nil {
		c.Close()
		c.err = err
	}
	return err
}

func (c *cursor) Close() os.Error {
	if c.err != nil {
		c.conn.responses[c.requestId] = nil, false
		if c.cursorId != 0 {
			c.conn.killCursors(c.cursorId)
		}
		c.err = os.NewError("mongo: cursor closed")
	}
	return nil
}

func (c *cursor) fill() {
	if c.err != nil {
		return
	}
	if c.requestId == 0 && c.cursorId != 0 {
		var err os.Error
		c.requestId, err = c.conn.getMore(c.namespace, 0, c.cursorId)
		if err != nil {
			c.fatal(err)
			return
		}
	}
	if c.requestId != 0 && c.count == 0 {
		r, err := c.conn.wait(c.requestId)
		if err != nil {
			c.fatal(err)
			return
		}
		c.count = r.count
		c.cursorId = r.cursorId
		c.data = r.data
		c.requestId = 0
		if c.cursorId != 0 {
			c.requestId, err = c.conn.getMore(c.namespace, 0, c.cursorId)
			if err != nil {
				c.fatal(err)
				return
			}
		}
	}
}

func (c *cursor) HasNext() bool {
	c.fill()
	return c.count > 0
}

func (c *cursor) Next(value interface{}) os.Error {
	c.fill()
	if c.err != nil {
		return c.err
	}
	if c.count == 0 {
		return EOF
	}
	if len(c.data) < 4 {
		return c.fatal(os.NewError("mongo: response data corrupted"))
	}
	n := int(wire.Uint32(c.data[0:4]))
	if n > len(c.data) {
		return c.fatal(os.NewError("mongo: response data corrupted"))
	}
	err := Decode(c.data[0:n], value)
	c.data = c.data[n:]
	c.count -= 1
	return err
}
