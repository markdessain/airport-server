package dremio

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"airportserver/sources"

	"github.com/apache/arrow/go/v16/arrow"
	arrowflight "github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/pelletier/go-toml/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func NewDremio(config []byte) sources.Source {
	var cfg Config
	err := toml.Unmarshal(config, &cfg)
	if err != nil {
		panic(err)
	}

	cfg.OutputDirectory = strings.TrimPrefix(cfg.OutputDirectory, "./")
	cfg.OutputDirectory = strings.TrimSuffix(cfg.OutputDirectory, "/")

	return sources.Source{Inner: Dremio{config: cfg}}
}

type Config struct {
	Name            string   `toml:"name"`
	RestHost        string   `toml:"rest_host"`
	RestPort        int      `toml:"rest_port"`
	FlightHost      string   `toml:"flight_host"`
	FlightPort      int      `toml:"flight_port"`
	Username        string   `toml:"username"`
	Password        string   `toml:"password"`
	Schemas         []string `toml:"schemas"`
	UseSSL          bool     `toml:"use_ssl"`
	OutputDirectory string   `toml:"output_directory"`
	SampleSize      int      `toml:"sample_size"`
}

type Dremio struct {
	config Config
}

func (d Dremio) DownloadCatalog(ctx context.Context) {

	restClient := NewClient(d.config)

	log.Println("Connecting to Dremio...")
	if err := restClient.Login(); err != nil {
		log.Fatalf("failed to login to Dremio: %v", err)
	}

	for _, schema := range d.config.Schemas {
		log.Println("Fetching tables...")
		tables, err := restClient.ListTables(schema)
		if err != nil {
			log.Fatalf("failed to list tables: %v", err)
		}

		if len(tables) == 0 {
			log.Println("No tables found.")
		}

		fmt.Printf("Found %d tables, testing accessibility...\n\n", len(tables))

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "SCHEMA\tTABLE\tTYPE\tSTATUS")
		fmt.Fprintln(w, "------\t-----\t----\t------")

		okCount := 0
		errorCount := 0

		for i, table := range tables {
			status := "OK"
			if err := restClient.TestTable(table.Schema, table.Name); err != nil {
				status = "ERROR"
				errorCount++
			} else {
				okCount++
				DownloadCatalogFile(d.config, table.Schema, table.Name)
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", table.Schema, table.Name, table.Type, status)

			if (i+1)%10 == 0 {
				fmt.Printf("Tested %d/%d tables...\n", i+1, len(tables))
			}
		}

		w.Flush()

		log.Printf("\nSummary:\n")
		log.Printf("Total tables: %d\n", len(tables))
		log.Printf("Accessible: %d\n", okCount)
		log.Printf("Errors: %d\n", errorCount)
	}

}

func (d Dremio) Stream(ctx context.Context, cancel context.CancelFunc, query string) (chan arrow.Record, error) {
	c := make(chan arrow.Record)
	go func() {
		<-ctx.Done()
		close(c)
	}()

	go func() {
		client, err := arrowflight.NewClientWithMiddleware(
			d.config.FlightHost+":"+strconv.Itoa(d.config.FlightPort),
			nil,
			nil,
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer client.Close()

		if ctx, err = client.AuthenticateBasicToken(ctx, d.config.Username, d.config.Password); err != nil {
			log.Fatal(err)
		}

		desc3 := &arrowflight.FlightDescriptor{
			Type: arrowflight.DescriptorCMD,
			Cmd:  []byte(query),
		}

		info, err := client.GetFlightInfo(ctx, desc3)
		if err != nil {
			log.Fatal(err)
		}

		stream2, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			log.Fatal(err)
		}
		rdr2, err := arrowflight.NewRecordReader(stream2)
		if err != nil {
			log.Fatal(err)
		}
		defer rdr2.Release()

		var previewRec arrow.Record

		for rdr2.Next() {
			rec := rdr2.Record()
			rec.Retain()
			if ctx.Err() == nil {
				c <- rec
			}
			if previewRec != nil {
				previewRec.Release()
			}

			previewRec = rec
		}

		if previewRec != nil {
			previewRec.Release()
		}

		log.Println("Query Completed")
		cancel()

	}()

	return c, nil
}

func (d Dremio) Tables(ctx context.Context) []string {
	return sources.Tables(d.config.OutputDirectory)
}

func (d Dremio) Schema(ctx context.Context, table string) (*arrow.Schema, error) {
	return sources.Schema(d.config.OutputDirectory, table)
}

func (d Dremio) Preview(ctx context.Context, cancel context.CancelFunc, table string) (chan arrow.Record, error) {
	return sources.Preview(ctx, cancel, d.config.OutputDirectory, table)
}
