package server

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v16/arrow/flight"
	flightpb "github.com/apache/arrow/go/v16/arrow/flight/gen/flight"
	"github.com/vmihailenco/msgpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func getDescriptor(a map[string]interface{}) string {

	change := func(a map[string]interface{}) string {
		return fmt.Sprintf("%s", a["descriptor"])
	}

	change2 := func(a string) string {
		return a
	}

	change3 := func(a string) string {
		return a[4:len(a)]
	}

	b := change(a)
	b = change2(b)
	b = change3(b)
	return b
}

func (s *SimpleFlightServer) handleEndpoints(stream flight.FlightService_DoActionServer, action *flight.Action) error {

	var a map[string]interface{}
	err := msgpack.Unmarshal(action.GetBody(), &a)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to unmarshal ticket: %v", err)
	}

	b, err := json.Marshal(a["parameters"])
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal parameters: %v", err)
	}

	var c map[string]interface{}

	err = json.Unmarshal(b, &c)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal parameters: %v", err)
	}

	d, err := json.Marshal(c["json_filters"])
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal parameters: %v", err)
	}
	jsonFilters := string(d)

	e, err := json.Marshal(c["column_ids"])
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal parameters: %v", err)
	}
	var columnIds []int
	err = json.Unmarshal(e, &columnIds)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal parameters: %v", err)
	}

	flightName := getDescriptor(a)

	tx := &TicketData{FlightName: flightName, JsonFilters: jsonFilters, ColumnIds: columnIds}

	packed1, err := msgpack.Marshal(tx)
	if err != nil {
		log.Fatal(err)
	}

	ddd := &flightpb.FlightEndpoint{Ticket: &flightpb.Ticket{Ticket: packed1}, Location: []*flightpb.Location{{Uri: "arrow-flight-reuse-connection://?"}}}

	g, err := proto.Marshal(ddd)
	if err != nil {
		log.Fatal(err)
	}
	z := []string{string(g)}

	packed2, err := msgpack.Marshal(z)
	if err != nil {
		log.Fatal(err)
	}

	stream.Send(&flight.Result{
		Body: packed2,
	})
	return nil
}
