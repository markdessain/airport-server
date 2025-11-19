package server

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v16/arrow/flight"
	flightpb "github.com/apache/arrow/go/v16/arrow/flight/gen/flight"
	"github.com/vmihailenco/msgpack"
)

func (s *SimpleFlightServer) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	fmt.Println("Action: ListFlights")
	catalog, schemaName := getCatalogAndSchema(stream.Context())
	for name, source := range s.config.Sources {

		if name == schemaName {
			for _, table := range source.Tables(context.Background()) {
				schema, err := source.Schema(context.Background(), table)
				if err != nil {
					fmt.Println(err)
				}

				appMetaData := AppMetadata{
					Type:      "table",
					Catalog:   catalog,
					Schema:    name,
					Name:      table,
					Comment:   "",
					ExtraData: nil,
				}
				packed, err := msgpack.Marshal(appMetaData)
				if err != nil {
					log.Fatal(err)
				}

				schemaBytes := flight.SerializeSchema(schema, s.alloc)

				flightInfo := &flight.FlightInfo{
					Schema: schemaBytes,
					FlightDescriptor: &flight.FlightDescriptor{
						Type: flightpb.FlightDescriptor_PATH,
						Path: []string{schemaName + "/" + table},
					},
					Endpoint: []*flight.FlightEndpoint{
						&flight.FlightEndpoint{
							Location: []*flight.Location{{Uri: "arrow-flight-reuse-connection://?"}},
						},
					},
					TotalRecords: 0,
					TotalBytes:   -1,
					AppMetadata:  packed,
				}

				stream.Send(flightInfo)
			}
		}
	}

	return nil
}
