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
	"net"
	"os"
	"strings"
	"io"
	"fmt"
)

const (
	//Set when getMore is called but the cursor id is not valid at the server.
	//Returned with zero results.
	responseCursorNotFound = 1 << 0

	//Set when query failed. Results consist of one document containing an
	//"$err" field describing the failure.
	responseQueryFailure = 1 << 1
)

type response struct {
	requestId uint32
	cursorId  uint64
	flags     int
	count     int
	data      []byte
}

func (r *response) error() os.Error {
	if r.flags&responseCursorNotFound != 0 {
		return os.NewError("mongo: cursor not found")
	}
	if r.flags&responseQueryFailure != 0 {
		m := make(map[string]interface{})
		err := Decode(r.data, &m)
		if err != nil {
			return os.NewError("mongo: query failure")
		}
		s, ok := m["$err"].(string)
		if !ok {
			return os.NewError("mongo: query failure")
		}
		return os.NewError(s)
	}
	return nil
}

type connection struct {
	conn      *net.TCPConn
	addr      string
	requestId uint32
	responses map[uint32]interface{}
}

// Dial connects to server at addr.
func Dial(addr string) (Conn, os.Error) {
	if strings.LastIndex(addr, ":") <= strings.LastIndex(addr, "]") {
		addr = addr + ":27017"
	}
	c := connection{addr: addr, responses:make(map[uint32]interface{})}
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

// nextId returns the next request id for this connection.
func (c *connection) nextId() uint32 {
	c.requestId += 1
	return c.requestId
}

// Close closes the connection to the server.
func (c *connection) Close() os.Error {
	var err os.Error
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	return err
}

// send sets the messages length and writes the message to the socket.
func (c *connection) send(msg []byte) os.Error {
	wire.PutUint32(msg[0:4], uint32(len(msg)))
	_, err := c.conn.Write(msg)
	return err
}

// receive recieves a single response from the server.
func (c *connection) receive() (*response, os.Error) {
	var r response

	var buf [36]byte
	if _, err := io.ReadFull(c.conn, buf[:]); err != nil {
		return nil, err
	}

	messageLength := int32(wire.Uint32(buf[0:4]))   // messageLength
	// buf[4:8]                                     // requestId
	r.requestId = wire.Uint32(buf[8:12])            // responseTo 
	opCode := int32(wire.Uint32(buf[12:16]))        // opCode
	r.flags = int(wire.Uint32(buf[16:20]))          // responseFlags
	r.cursorId = wire.Uint64(buf[20:28])            // cursorId
	// buf[28:32]                                   // startingFrom
	r.count = int(wire.Uint32(buf[32:36]))          // numberReturned
	r.data = make([]byte, messageLength-36)
	if _, err := io.ReadFull(c.conn, r.data); err != nil {
		return nil, err
	}
	if opCode != 1 {
		return nil, os.NewError(fmt.Sprintf("mongo: unknown reponse message opcode %d", opCode))
	}
	return &r, nil
}

// wait waits for a response to the given request. Reponses to other requests
// are saved.
func (c *connection) wait(requestId uint32) (*response, os.Error) {
	r := c.responses[requestId]
	c.responses[requestId] = nil, false

	switch r := r.(type) {
	case *response:
		return r, nil
	case nil:
		return nil, os.NewError("mongo: not expecting response")
	default:
		for {
			r, err := c.receive()
			if err != nil {
				return nil, err
			}
			if r.requestId == requestId {
				return r, nil
			} else if _, found := c.responses[r.requestId]; found {
				c.responses[r.requestId] = r
			} else if r.cursorId != 0 {
				c.killCursors(r.cursorId)
			}
		}
	}
	return nil, os.NewError("mongo: unexpeted")
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

func (c *connection) query(namespace string, flags, numberToSkip, numberToReturn int, query, returnFieldSelector interface{}) (uint32, os.Error) {
	requestId := c.nextId()
	b := buffer(make([]byte, 0, 512))
	b.Next(4)                    // placeholder for message length
	b.WriteUint32(requestId)     // requestId
	b.WriteUint32(0)             // responseTo
	b.WriteUint32(2004)          // opCode
	b.WriteUint32(uint32(flags)) // flags
	b.WriteString(namespace)     // namespace
	b.WriteByte(0)               // null terminator
	b.WriteUint32(uint32(numberToSkip))
	b.WriteUint32(uint32(numberToReturn))
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

func (c *connection) Update(namespace string, document, selector interface{}, flags int) (err os.Error) {
	b := buffer(make([]byte, 0, 512))
	b.Next(4)                    // placeholder for message length
	b.WriteUint32(c.nextId())    // requestId
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

func (c *connection) Remove(namespace string, selector interface{}, flags int) (err os.Error) {
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

func (c *connection) FindOne(namespace string, query, returnFieldSelector interface{}, flags int, result interface{}) os.Error {
	requestId, err := c.query(namespace, flags, 0, 1, query, returnFieldSelector)
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
	err = r.error()
	if err != nil {
		return err
	}
	if r.count < 1 {
		return EOF
	}
	return Decode(r.data, result)
}

