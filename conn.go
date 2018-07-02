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
	"github.com/lunny/log"
	"net"
	"sync"
	"time"
)

// conn is the low-level implementation of Conn
type conn struct {
	mu           sync.Mutex
	err          error
	conn         net.Conn
	bw           *bufio.Writer
	writeTimeout time.Duration
}

// NewConn returns a new connection for the given net connection.
func NewConn(netConn net.Conn, writeTimeout time.Duration) (Conn, error) {
	return &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		writeTimeout: writeTimeout,
	}, nil
}

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		c.conn.Close()
	}
	c.mu.Unlock()
	return err
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

func (c *conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *conn) Pub(topic string, message []byte) error {
	if len(topic) == 0 || message == nil {
		return c.fatal(errors.New("topic and message should not be empty"))
	}

	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	log.Println(topic, string(message))

	_, err := c.bw.Write(message)
	if err != nil {
		return c.fatal(err)
	}

	err = c.bw.Flush()
	if err != nil {
		return c.fatal(err)
	}

	return nil
}
