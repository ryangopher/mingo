// Copyright 2012 Gary Burd
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

package mingo

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// conn is the low-level implementation of Conn
type conn struct {

	// Shared
	mu   sync.Mutex
	err  error
	conn net.Conn

	idleTime time.Time

	// Read
	readTimeout time.Duration
	br          *bufio.Reader

	// Write
	writeTimeout time.Duration
	bw           *bufio.Writer
}

func (c *conn) Close() error {
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("mingo: closed")
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		// Close connection to force errors on subsequent calls and to unblock
		// other reader or writer.
		c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *conn) writeString(s string) error {
	c.bw.WriteString(s)
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *conn) writeBytes(p []byte) error {
	c.bw.Write(p)
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *conn) writeCommand(cmd []byte, args []interface{}) error {
	return c.writeBytes(cmd)
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("mingo: %s", string(pe))
}

func (c *conn) readLine() ([]byte, error) {
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("long response line")
	}

	if err != nil {
		return nil, err
	}

	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}

	return p[:i], nil
}

func (c *conn) readReply() (interface{}, error) {
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, protocolError("short response line")
	}

	return nil, protocolError("unexpected response line")
}

func (c *conn) Send(cmd []byte, args ...interface{}) error {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	err := c.writeCommand(cmd, args)
	if err != nil {
		return c.fatal(err)
	}

	return nil
}

func (c *conn) Flush() error {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	err := c.bw.Flush()
	if err != nil {
		return c.fatal(err)
	}

	return nil
}

func (c *conn) Receive() (reply interface{}, err error) {
	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	reply, err = c.readReply()
	if err != nil {
		return nil, c.fatal(err)
	}

	err, ok := reply.(Error)
	if ok {
		return nil, err
	}

	return
}

func (c *conn) Post(cmd []byte, args ...interface{}) (interface{}, error) {
	if len(cmd) == 0 {
		return nil, nil
	}

	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	err := c.writeCommand(cmd, args)
	if err != nil {
		return nil, c.fatal(err)
	}

	err = c.bw.Flush()
	if err != nil {
		return nil, c.fatal(err)
	}

	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	reply, err := c.readReply()
	if err != nil {
		return nil, c.fatal(err)
	}

	e, ok := reply.(Error)
	if ok && err == nil {
		err = e
	}

	return reply, err
}

// MarkIdleTime mark time becoming idle.
func (c *conn) MarkIdleTime() {
	c.idleTime = time.Now()
}

// GetIdleTime get time becoming idle.
func (c *conn) GetIdleTime() time.Time {
	return c.idleTime
}

// NewConn returns a new connection for the given net connection.
func NewConn(netConn net.Conn, do *dialOptions) (Conn, error) {
	return &conn{
		conn:         netConn,
		bw:           bufio.NewWriterSize(netConn, do.bwSize),
		br:           bufio.NewReaderSize(netConn, do.brSize),
		readTimeout:  do.readTimeout,
		writeTimeout: do.writeTimeout,
	}, nil
}
