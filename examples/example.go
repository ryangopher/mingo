package main

import (
	"github.com/mingo"
	"log"
	"time"
)

func main() {
	pool := mingo.Pool{
		Dial: func() (mingo.Conn, error) {
			conn, err := mingo.Dial("tcp", "127.0.0.1:4151", mingo.NewConn)
			if err != nil {
				return nil, err
			}

			return conn, err
		},

		TestOnBorrow: nil,
		MaxActive:    1024,
		MaxIdle:      1000,
		IdleTimeout:  60 * time.Second,
		Wait:         true,
	}

	conn := pool.Get()
	defer conn.Close()

	err := conn.Send("1234567890")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Send("1234567890")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Send("1234567890")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Send("1234567890")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Send("1234567890")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Flush()
	if err != nil {
		log.Fatal(err)
	}

	pool.Close()
}
