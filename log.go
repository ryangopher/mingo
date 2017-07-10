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
	"bytes"
	"fmt"
	"log"
	"time"
)

// NewLoggingConn returns a logging wrapper around a connection.
func NewLoggingConn(conn Conn, logger *log.Logger, prefix string) Conn {
	if prefix != "" {
		prefix = prefix + "."
	}
	return &loggingConn{Conn: conn, logger: logger, prefix: prefix}
}

type loggingConn struct {
	Conn
	logger   *log.Logger
	prefix   string
	idleTime time.Time
}

func (c *loggingConn) Close() error {
	err := c.Conn.Close()
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%sClose() -> (%v)", c.prefix, err)
	c.logger.Output(2, buf.String())
	return err
}

func (c *loggingConn) printValue(buf *bytes.Buffer, v interface{}) {
	const chop = 32
	switch v := v.(type) {
	case []byte:
		if len(v) > chop {
			fmt.Fprintf(buf, "%q...", v[:chop])
		} else {
			fmt.Fprintf(buf, "%q", v)
		}
	case string:
		if len(v) > chop {
			fmt.Fprintf(buf, "%q...", v[:chop])
		} else {
			fmt.Fprintf(buf, "%q", v)
		}
	case []interface{}:
		if len(v) == 0 {
			buf.WriteString("[]")
		} else {
			sep := "["
			fin := "]"
			if len(v) > chop {
				v = v[:chop]
				fin = "...]"
			}
			for _, vv := range v {
				buf.WriteString(sep)
				c.printValue(buf, vv)
				sep = ", "
			}
			buf.WriteString(fin)
		}
	default:
		fmt.Fprint(buf, v)
	}
}

func (c *loggingConn) debug(method string, commandName []byte, args []interface{}, reply interface{}, err error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s%s(", c.prefix, method)
	if method != "Receive" {
		buf.Write(commandName)
		for _, arg := range args {
			buf.WriteString(", ")
			c.printValue(&buf, arg)
		}
	}
	buf.WriteString(") -> (")
	if method != "Send" {
		c.printValue(&buf, reply)
		buf.WriteString(", ")
	}
	fmt.Fprintf(&buf, "%v)", err)
	c.logger.Output(3, buf.String())
}

func (c *loggingConn) Post(command []byte, args ...interface{}) (interface{}, error) {
	response, err := c.Conn.Post(command, args...)
	c.debug("Do", command, args, response, err)
	return response, err
}

func (c *loggingConn) Send(command []byte, args ...interface{}) error {
	err := c.Conn.Send(command, args...)
	c.debug("Send", command, args, nil, err)
	return err
}

func (c *loggingConn) Receive() (interface{}, error) {
	reply, err := c.Conn.Receive()
	c.debug("Receive", []byte(""), nil, reply, err)
	return reply, err
}

// MarkIdleTime mark time becoming idle.
func (c *loggingConn) MarkIdleTime() {
	c.idleTime = time.Now()
}

// GetIdleTime get time becoming idle.
func (c *loggingConn) GetIdleTime() time.Time {
	return c.idleTime
}
