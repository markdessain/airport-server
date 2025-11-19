package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow/flight"
)

type middle struct {
}

func (m middle) StartCall(ctx context.Context) context.Context {
	fmt.Println("StartCall")
	fmt.Println(ctx)
	// transport.GetConnection
	// if ctx.Value("ABC") == nil {
	// 	fmt.Println("Set context value")
	// 	ctx = context.WithValue(ctx, "ABC", time.Now())
	// }

	return ctx
}
func (m middle) CallCompleted(ctx context.Context, err error) {
	fmt.Println("CallCompleted")
	// fmt.Println(ctx)
	// fmt.Println(err)
}

type serverAuth struct{}

func (sa *serverAuth) Authenticate(c flight.AuthConn) error {
	fmt.Println("Token Authenticate")
	return c.Send([]byte("foobar"))
}

func (sa *serverAuth) IsValid(token string) (interface{}, error) {
	fmt.Println("Token IsValid")
	fmt.Println(token)
	return "a", nil
}

func (sa *serverAuth) Validate(username string, password string) (string, error) {
	fmt.Println("CCC")
	return "", nil
}
