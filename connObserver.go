package main

import (
	"container/list"
	"fmt"
	"net"
)

type Observable struct {
	subs *list.List
}

func (o *Observable) Subscribe(x Observer) {
	o.subs.PushBack(x)
}

func (o *Observable) Unsubscribe(x Observer) {
	for z := o.subs.Front(); z != nil; z = z.Next() {
		if z.Value.(Observer) == x {
			o.subs.Remove(z)
		}
	}
}

func (o *Observable) Fire(data interface{}) {
	for z := o.subs.Front(); z != nil; z = z.Next() {
		z.Value.(Observer).Notify(data)
	}
}

type Observer interface {
	Notify(data interface{})
}

// whenever a person catches a cold,
// a doctor must be called
type Message struct {
	Observable
	Connection net.Conn
}

func NewConnection(conn net.Conn) *Message {
	return &Message{
		Observable: Observable{new(list.List)},
		Connection: conn,
	}
}

type ConnectionService struct{}

func (d *ConnectionService) Notify(data interface{}) {
	fmt.Printf("A connection has been called for message %s\n",
		data.(string))

	dataElement := parseIsoMsg(data.(string))
	conn, ok := connectionMap[dataElement[41]]
	if ok {
		fmt.Println("connection detail: ", conn.LocalAddr(), conn.RemoteAddr())
		delete(connectionMap, dataElement[41])

		responseMessage(conn, data.(string))
		// conn.Close()
	}

}
