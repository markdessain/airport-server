package server

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
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

	a := strings.SplitN(t.FlightName, "/", 3)

	if len(a) != 3 {
		return status.Errorf(codes.Internal, "invalid flight name: %s", t.FlightName)
	}
	catalog := a[0]
	schema := a[1]
	table := a[2]

	b := s.config.Sources[schema]

	var records chan arrow.Record

	requestCtx, requestCancel := context.WithCancel(context.Background())

	go func() {
		for {
			if s.ctx.Err() != nil {
				fmt.Println("Server Disconnected")
				requestCancel()
				return
			}

			if stream.Context().Err() != nil {
				fmt.Println("Client Disconnected")
				requestCancel()
				return
			}

			time.Sleep(1 * time.Second)
		}

	}()

	if catalog == "preview" {
		records, err = b.Preview(requestCtx, requestCancel, table)
	} else {
		records, err = b.Stream(requestCtx, requestCancel, "SELECT * FROM "+table)
	}

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
			requestCancel()
			<-records
			break
		}
	}

	if writer != nil {
		err = writer.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}
