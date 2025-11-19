package dremio

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// go run main.go -address "..." -username "..." -password ".." -query "SELECT * FROM ..." -limit 100 -columns 'a,b'
func DownloadCatalogFile(config Config, tableSchema string, table string) {

	ctx := context.Background()

	address := config.FlightHost + ":" + strconv.Itoa(config.FlightPort)
	query := "SELECT * FROM \"" + tableSchema + "\".\"" + table + "\""

	if config.SampleSize > 0 {
		query = query + " LIMIT " + strconv.Itoa(config.SampleSize)
	} else {
		query = query + " LIMIT 0"
	}

	flag.Parse()

	client, err := flight.NewClientWithMiddleware(
		address,
		nil,
		nil,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if ctx, err = client.AuthenticateBasicToken(ctx, config.Username, config.Password); err != nil {
		log.Fatal(err)
	}

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(query),
	}

	err = os.MkdirAll(config.OutputDirectory+"/schemas", os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	err = os.MkdirAll(config.OutputDirectory+"/data", os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.OpenFile(config.OutputDirectory+"/schemas/"+tableSchema+"."+table, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}
	file2, err := os.OpenFile(config.OutputDirectory+"/data/"+tableSchema+"."+table, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Getting Schema")
	sc, err := client.GetSchema(ctx, desc)
	if err != nil {
		log.Fatal(err)
	}

	_, err = file.Write(sc.GetSchema())
	if err != nil {
		log.Fatal(err)
	}

	if config.SampleSize > 0 {

		var writer *pqarrow.FileWriter

		fmt.Println("Getting Results")
		info, err := client.GetFlightInfo(ctx, desc)
		if err != nil {
			log.Fatal(err)
		}

		stream, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			log.Fatal(err)
		}
		rdr, err := flight.NewRecordReader(stream)
		if err != nil {
			log.Fatal(err)
		}
		defer rdr.Release()

		for rdr.Next() {

			rec := rdr.Record()

			if writer == nil {
				writer, err = pqarrow.NewFileWriter(rec.Schema(), file2, nil, pqarrow.DefaultWriterProps())
				if err != nil {
					log.Fatal(err)
				}
			}

			defer rec.Release()
			err = writer.Write(rec)
			if err != nil {
				log.Fatal(err)
			}
		}

		err = writer.Close()
		if err != nil {
			panic(err)
		}

	}

}
