package server

import (
	"context"
	"fmt"
	"log"

	"airportserver/config"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/vmihailenco/msgpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func Launch(ctx context.Context, config config.Config) {

	server := NewSimpleFlightServer(config)

	grpcServer2 := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		// flight.CreateServerBasicAuthMiddleware(&serverAuth{}),
		// flight.CreateServerMiddleware(middle{}),
	})

	grpcServer2.RegisterFlightService(server)

	err := grpcServer2.Init("localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		<-ctx.Done()
		log.Println("Graceful stopping ...")
		grpcServer2.Shutdown()
		log.Println("Graceful shutdown completed.")
	}()

	log.Println("Starting Apache Flight server running on :8080")
	err = grpcServer2.Serve()
	if err != nil {
		log.Println("Failed to serve:", err)
	}

	err = grpcServer2.Serve()
	if err != nil {
		log.Fatal(err)
	}

}

func NewSimpleFlightServer(config config.Config) *SimpleFlightServer {
	return &SimpleFlightServer{
		alloc:        memory.NewGoAllocator(),
		transactions: make(map[string]*Transaction),
		config:       config,
	}
}

// SimpleFlightServer implements the Flight service
type SimpleFlightServer struct {
	flight.BaseFlightServer
	alloc        memory.Allocator
	transactions map[string]*Transaction
	config       config.Config
}

// DoAction handles action requests
func (s *SimpleFlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	fmt.Println("Action: " + action.Type)

	switch action.Type {
	case "list_schemas":
		return s.handleListSchemas(stream)
	case "create_transaction":
		return s.handleCreateTransaction(stream)
	case "catalog_version":
		return s.handleCatalogVersion(stream)
	case "endpoints":
		return s.handleEndpoints(stream, action)
	default:
		return status.Errorf(codes.Unimplemented, "unknown action type: %s", action.Type)
	}
}

func (s *SimpleFlightServer) handleCreateTransaction(stream flight.FlightService_DoActionServer) error {
	tx := &Catalog{CatalogName: "hello3"}

	packed2, err := msgpack.Marshal(tx)
	if err != nil {
		log.Fatal(err)
	}

	stream.SendMsg(&flight.Result{
		Body: packed2,
	})
	return nil
}

func (s *SimpleFlightServer) handleCatalogVersion(stream flight.FlightService_DoActionServer) error {
	tx := &VersionInfo{CatalogVersion: 1, IsFixed: false}

	packed2, err := msgpack.Marshal(tx)
	if err != nil {
		log.Fatal(err)
	}

	stream.SendMsg(&flight.Result{
		Body: packed2,
	})
	return nil
}
