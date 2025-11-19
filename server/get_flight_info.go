package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow/flight"
	flightpb "github.com/apache/arrow/go/v16/arrow/flight/gen/flight"
)

func (s *SimpleFlightServer) GetFlightInfo(ctx context.Context, ticket *flightpb.FlightDescriptor) (*flightpb.FlightInfo, error) {
	fmt.Println("Action: GetFlightInfo")
	fmt.Println(ticket)
	fmt.Println(ctx)

	// s.config.Sources[]

	// appMetaData := AppMetadata{
	// 	// Type:      "table",
	// 	// Catalog:   catalog,
	// 	// Schema:    schema,
	// 	// Name:      table,
	// 	// Comment:   "",
	// 	// ExtraData: nil,
	// }
	// packed, err := msgpack.Marshal(appMetaData)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	fmt.Println(string(ticket.GetCmd()))
	// fmt.Println("A")
	// fmt.Println(s.modifySQL(string(ticket.GetCmd())))
	// cmd := exec.Command("duckdb-binary", "-c", "COPY ("+s.modifySQL(string(ticket.GetCmd()))+") TO './data/query.parquet' (FORMAT parquet);")
	// _, err = cmd.Output()
	// if err != nil {
	// 	return nil, status.Errorf(codes.Internal, "failed to execute command: %v", err)
	// }

	// fmt.Println("B")
	// f, err := os.Open("./data/query.parquet")
	// if err != nil {
	// 	return nil, status.Errorf(codes.Internal, "failed to open parquet file: %v", err)
	// }
	// reader, err := file.NewParquetReader(f)
	// if err != nil {
	// 	return nil, status.Errorf(codes.Internal, "failed to open read file: %v", err)
	// }

	// defer reader.Close()

	// pqReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 1}, memory.DefaultAllocator)
	// if err != nil {
	// 	return nil, status.Errorf(codes.Internal, "failed to NewFileReader file: %v", err)
	// }

	// recordReader, err := pqReader.GetRecordReader(context.Background(), nil, nil)
	// if err != nil {
	// 	return nil, status.Errorf(codes.Internal, "failed to GetRecordReader file: %v", err)
	// }

	// arrowSchema := recordReader.Schema()
	// schemaBytes := flight.SerializeSchema(arrowSchema, s.alloc)

	return &flight.FlightInfo{}, nil
	// 	Schema: schemaBytes, // Start with no schema to avoid serialization issues
	// 	FlightDescriptor: &flight.FlightDescriptor{
	// 		Type: flightpb.FlightDescriptor_CMD,
	// 		Cmd:  ticket.Cmd,
	// 	},
	// 	Endpoint: []*flight.FlightEndpoint{
	// 		{
	// 			// Ticket: &flight.Ticket{Ticket: []byte("static")},
	// 		},
	// 	},
	// 	TotalRecords: 0,
	// 	TotalBytes:   -1,
	// 	AppMetadata:  packed,
	// }, nil
}
