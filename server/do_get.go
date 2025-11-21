package server

import (
	"context"
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
	log.Println("Action: DoGet")
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
		<-s.ctx.Done()
		log.Println("Server Disconnected")
		requestCancel()
	}()

	go func() {
		<-stream.Context().Done()
		log.Println("Client Disconnected")
		requestCancel()
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

	// for rec := range drip(requestCtx, records) {
	for rec := range records {

		if writer == nil {
			writer = flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))
			defer func() {
				if err := writer.Close(); err != nil {
					log.Printf("Error closing writer: %v", err)
				}
			}()
		}

		if rec.NumRows() > 0 {
			err = writer.Write(rec)
			if err != nil {
				requestCancel()
				<-records
				break
			}
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

func drip(ctx context.Context, c chan arrow.Record) chan arrow.Record {

	c3 := make(chan arrow.Record, 1)

	go func() {

		c2 := make(chan arrow.Record, 1)

		go func() {
			<-ctx.Done()
			close(c2)
		}()

		var processing arrow.Record

		running := true

		go func() {
			for x := range c {
				c2 <- x
			}
			running = false
		}()

		processIndex := 0
		processChunk := 100
		for running {
			select {
			case x, ok := <-c2:
				if ok {
					if processing != nil {
						c3 <- processing.NewSlice(int64(processIndex), int64(processing.NumRows()))
						processing.Release()
					}
					processIndex = 0
					processing = x
					processing.Retain()
				} else {
					running = false
				}
			default:
				if processing != nil && processIndex < int(processing.NumRows()) {

					if processIndex+processChunk >= int(processing.NumRows()) {
						c3 <- processing.NewSlice(int64(processIndex), int64(processing.NumRows()))
						processIndex = int(processing.NumRows())
					} else {
						c3 <- processing.NewSlice(int64(processIndex), int64(processIndex+processChunk))
						processIndex += processChunk
					}
				}
			}

			time.Sleep(time.Second * 1)
		}

		if ctx.Err() == nil {
			c3 <- processing.NewSlice(int64(processIndex), int64(processing.NumRows()))
			processIndex = 0

			processing = <-c2

			c3 <- processing.NewSlice(int64(processIndex), int64(processing.NumRows()))

		}
		close(c3)

	}()

	return c3
}
