package server

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/vmihailenco/msgpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *SimpleFlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	fmt.Println("Action: DoGet")
	var t TicketData
	err := msgpack.Unmarshal(ticket.GetTicket(), &t)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to unmarshal ticket: %v", err)
	}

	a := strings.SplitN(t.FlightName, "/", 2)

	if len(a) != 2 {
		return status.Errorf(codes.Internal, "invalid flight name: %s", t.FlightName)
	}
	schema := a[0]
	table := a[1]

	b := s.config.Sources[schema]

	records, err := b.Stream(context.Background(), "SELECT * FROM "+table)

	if err != nil {
		return status.Errorf(codes.Internal, "failed to get records: %v", err)
	}

	var writer *flight.Writer

	for rec := range records {

		if writer == nil {
			writer = flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))
			defer func() {
				if err := writer.Close(); err != nil {
					log.Printf("Error closing writer: %v", err)
				}
			}()
		}

		err = writer.Write(rec)
		if err != nil {
			fmt.Println(err)
			break
		}
	}

	err = writer.Close()
	if err != nil {
		log.Fatal(err)
	}

	return nil
}
