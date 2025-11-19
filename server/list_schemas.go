package server

import (
	"log"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *SimpleFlightServer) handleListSchemas(stream flight.FlightService_DoActionServer) error {
	schemas := []Schema{}

	for name, _ := range s.config.Sources {
		schemas = append(schemas, Schema{Name: name, Description: "", Tags: map[string]string{}, Content: Content{SHA256: ""}, IsDefault: false})
	}

	data := FlightData{
		Content: Content{
			SHA256: "",
		},
		Schemas: schemas,
		VersionInfo: VersionInfo{
			CatalogVersion: 1,
			IsFixed:        false,
		},
	}

	packed, err := msgpack.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	compressor, err := zstd.NewWriter(nil)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create compressor: %v", err)
	}
	defer compressor.Close()

	compressed := compressor.EncodeAll(packed, make([]byte, 0, len(packed)))

	var r []interface{}

	r = append(r, len(packed))
	r = append(r, compressed)

	packed2, err := msgpack.Marshal(r)
	if err != nil {
		log.Fatal(err)
	}

	stream.SendMsg(&flight.Result{
		Body: packed2,
	})

	return nil
}
