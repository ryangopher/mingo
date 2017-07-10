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
	"container/list"
	"errors"
	"log"
	"sync"
	"time"
)

var nowFunc = time.Now // for testing

// ErrPoolExhausted is returned from a pool connection method (Do, Send,
// Receive, Flush, Err) when the maximum number of database connections in the
// pool has been reached.
var ErrPoolExhausted = errors.New("mingo: connection pool exhausted")

var (
	errPoolClosed = errors.New("mingo: connection pool closed")
	errConnClosed = errors.New("mingo: connection closed")
)

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers using a package level variable. The pool configuration used
// here is an example, not a recommendation.
//
//  func newPool(addr string) *mingo.Pool {
//    return &mingo.Pool{
//      MaxIdle: 3,
//      IdleTimeout: 240 * time.Second,
//      Dial: func () (mingo.Conn, error) { return mingo.Dial("tcp", addr) },
//    }
//  }
//
//  var (
//    pool *mingo.Pool
//    mingoServer = flag.String("mingoServer", ":6379", "")
//  )
//
//  func main() {
//    flag.Parse()
//    pool = newPool(*mingoServer)
//    ...
//  }
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  func serveHome(w http.ResponseWriter, r *http.Request) {
//      conn := pool.Get()
//      defer conn.Close()
//      ...
//  }
//
// Use the Dial function to authenticate connections with the AUTH command or
// select a database with the SELECT command:
//
//  pool := &mingo.Pool{
//    // Other pool configuration not shown in this example.
//    Dial: func () (mingo.Conn, error) {
//      c, err := mingo.Dial("tcp", server)
//      if err != nil {
//        return nil, err
//      }
//      if _, err := c.Do("AUTH", password); err != nil {
//        c.Close()
//        return nil, err
//      }
//      if _, err := c.Do("SELECT", db); err != nil {
//        c.Close()
//        return nil, err
//      }
//      return c, nil
//    }
//  }
//
// Use the TestOnBorrow function to check the health of an idle connection
// before the connection is returned to the application. This example PINGs
// connections that have been idle more than a minute:
//
//  pool := &mingo.Pool{
//    // Other pool configuration not shown in this example.
//    TestOnBorrow: func(c mingo.Conn, t time.Time) error {
//      if time.Since(t) < time.Minute {
//        return nil
//      }
//      _, err := c.Do("PING")
//      return err
//    },
//  }
//
type Pool struct {

	// Dial is an application supplied function for creating and configuring a
	// connection.
	//
	// The connection returned from Dial must not be in a special state
	// (subscribed to pubsub channel, transaction started, ...).
	Dial func() (Conn, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c Conn, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// check idle conn and release timeout idle connections
	GCInterval time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// mark pool initialized
	initialized bool

	// mark previous clean time
	nextGCTime time.Time

	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

// NewPool creates a new pool.
//
// Deprecated: Initialize the Pool directory as shown in the example.
func NewPool(newFn func() (Conn, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle, nextGCTime: nowFunc()}
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *Pool) Get() Conn {
	c, err := p.get()
	if err != nil {
		return errorConnection{err: err}
	}
	return c
}

// ActiveCount returns the number of active connections in the pool.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// IdleCount returns the number of idle connections in the pool.
func (p *Pool) IdleCount() int {
	p.mu.Lock()
	idle := p.idle.Len()
	p.mu.Unlock()
	return idle
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(Conn).Close()
	}
	return nil
}

// release decrements the active count and signals waiters. The caller must
// hold p.mu during the call.
func (p *Pool) release() {
	p.active--
	if p.cond != nil {
		p.cond.Signal()
	}
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get() (Conn, error) {
	p.mu.Lock()

	if !p.initialized {
		p.initialized = true
		p.nextGCTime = nowFunc().Add(p.GCInterval)
		log.Printf("mingo : nextgctime %s", p.nextGCTime)
	}

	// Prune stale connections.
	// 不要频繁的去处理idle conn
	if timeout := p.IdleTimeout; (timeout > 0) && (nowFunc().After(p.nextGCTime)) {
		p.nextGCTime = nowFunc().Add(p.GCInterval)
		log.Printf("mingo : nextgctime %s", p.nextGCTime)

		//每次只处理一半的idle conn
		for i, n := 0, p.idle.Len(); i < int(float32(n)*0.5); i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			c := e.Value.(Conn)
			if c.GetIdleTime().Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			c.Close()
			p.mu.Lock()
		}
	}

	for {

		// Get idle connection.

		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			c := e.Value.(Conn)
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(c, c.GetIdleTime()) == nil {
				return c, nil
			}
			c.Close()
			p.mu.Lock()
			p.release()
		}

		// Check for pool closed before dialing a new connection.

		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("mingo: get on closed pool")
		}

		// Dial new connection if under limit.

		if p.MaxActive == 0 || p.active < p.MaxActive {
			dial := p.Dial
			p.active++
			p.mu.Unlock()
			c, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}
			return c, err
		}

		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}

		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

// Put put back conn to pool
func (p *Pool) Put(c Conn, forceClose bool) error {
	if c == nil {
		return nil
	}

	if _, ok := c.(errorConnection); ok {
		return nil
	}

	return p.put(c, forceClose)
}

func (p *Pool) put(c Conn, forceClose bool) error {
	err := c.Err()
	p.mu.Lock()
	if !p.closed && err == nil && !forceClose {
		c.MarkIdleTime()
		p.idle.PushFront(c)
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(Conn)
		} else {
			c = nil
		}
	}

	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}

	p.release()
	p.mu.Unlock()
	return c.Close()
}

type errorConnection struct {
	err      error
	idleTime time.Time
}

func (ec errorConnection) Post([]byte, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConnection) Send([]byte, ...interface{}) error                { return ec.err }
func (ec errorConnection) Err() error                                       { return ec.err }
func (ec errorConnection) Close() error                                     { return ec.err }
func (ec errorConnection) Flush() error                                     { return ec.err }
func (ec errorConnection) Receive() (interface{}, error)                    { return nil, ec.err }
func (ec errorConnection) MarkIdleTime()                                    { ec.idleTime = nowFunc() }
func (ec errorConnection) GetIdleTime() time.Time                           { return ec.idleTime }
