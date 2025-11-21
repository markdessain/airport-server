package server

import (
	"context"
	"log"

	"github.com/apache/arrow/go/v16/arrow/flight"
)

type middle struct {
}

func (m middle) StartCall(ctx context.Context) context.Context {
	log.Println("StartCall")
	log.Println(ctx)

	return ctx
}
func (m middle) CallCompleted(ctx context.Context, err error) {
	log.Println("CallCompleted")
}

type serverAuth struct{}

func (sa *serverAuth) Authenticate(c flight.AuthConn) error {
	log.Println("Token Authenticate")
	return c.Send([]byte("foobar"))
}

func (sa *serverAuth) IsValid(token string) (interface{}, error) {
	log.Println("Token IsValid")
	log.Println(token)
	return "a", nil
}

func (sa *serverAuth) Validate(username string, password string) (string, error) {
	log.Println("CCC")
	return "", nil
}
