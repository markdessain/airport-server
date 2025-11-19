package dremio

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"

	"airportserver/sources"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/flight"
	arrowflight "github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
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

	fmt.Println("Connecting to Dremio...")
	if err := restClient.Login(); err != nil {
		log.Fatalf("failed to login to Dremio: %v", err)
	}

	for _, schema := range d.config.Schemas {
		fmt.Println("Fetching tables...")
		tables, err := restClient.ListTables(schema)
		if err != nil {
			log.Fatalf("failed to list tables: %v", err)
		}

		if len(tables) == 0 {
			fmt.Println("No tables found.")
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

				fmt.Println("AA")
				DownloadCatalogFile(d.config, table.Schema, table.Name)
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", table.Schema, table.Name, table.Type, status)

			if (i+1)%10 == 0 {
				fmt.Printf("Tested %d/%d tables...\n", i+1, len(tables))
			}
		}

		w.Flush()

		fmt.Printf("\nSummary:\n")
		fmt.Printf("Total tables: %d\n", len(tables))
		fmt.Printf("Accessible: %d\n", okCount)
		fmt.Printf("Errors: %d\n", errorCount)
	}

}

func (d Dremio) Tables(ctx context.Context) []string {

	result := []string{}
	err := filepath.Walk(d.config.OutputDirectory+"/schemas/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}

		if !info.IsDir() {
			dpath := strings.TrimPrefix(path, d.config.OutputDirectory+"/schemas/")
			result = append(result, dpath)
		}

		return nil
	})

	if err != nil {
		log.Printf("Error walking directory: %v\n", err)
	}

	return result
}

func (d Dremio) Schema(ctx context.Context, table string) (*arrow.Schema, error) {

	f, err := os.Open(d.config.OutputDirectory + "/schemas/" + table)
	if err != nil {
		return nil, errors.New("failed to Open: " + err.Error())
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		return nil, errors.New("failed to read file: " + err.Error())
	}
	return flight.DeserializeSchema(data, memory.DefaultAllocator)

}
func (d Dremio) Preview(ctx context.Context, table string) (chan arrow.Record, error) {

	run := true
	c := make(chan arrow.Record)

	go func() {
		<-ctx.Done()
		close(c)
		run = false
	}()

	go func() {
		f, err := os.Open(d.config.OutputDirectory + "/data/" + table)
		if err != nil {
			fmt.Println("failed to Open: " + err.Error())
		}
		pf, err := file.NewParquetReader(f)
		if err != nil {
			fmt.Println("failed to NewParquetReader: " + err.Error())
		}
		defer pf.Close()
		reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1}, memory.NewGoAllocator())
		if err != nil {
			fmt.Println("failed to NewFileReader: " + err.Error())
		}

		rdr, err := reader.GetRecordReader(context.Background(), nil, nil)
		if err != nil {
			log.Fatal(err)
		}

		defer rdr.Release()
		var previewRec arrow.Record

		for rdr.Next() {
			rec := rdr.Record()
			rec.Retain()

			if !run {
				return
			}
			c <- rec

			if previewRec != nil {
				previewRec.Release()
			}
			previewRec = rec
		}

		close(c)
	}()

	return c, nil

}

func (d Dremio) Stream(ctx context.Context, query string) (chan arrow.Record, error) {

	run := true
	c := make(chan arrow.Record)

	go func() {
		<-ctx.Done()
		close(c)
		run = false
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

		ctx := context.Background()

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

			if !run {
				return
			}
			c <- rec

			if previewRec != nil {
				previewRec.Release()
			}
			previewRec = rec
		}

		close(c)

	}()

	return c, nil
}

func modifySQL(sql string, outputDirectory string) string {
	// Split the SQL into words while preserving whitespace and structure
	words := strings.Fields(sql)

	for i := 0; i < len(words); i++ {
		word := strings.ToUpper(words[i])

		// Check for FROM clause
		if word == "FROM" && i+1 < len(words) {
			tableName := words[i+1]
			// Remove trailing comma or other punctuation if present
			trimmed := strings.TrimRight(tableName, ",;)")
			suffix := tableName[len(trimmed):]
			words[i+1] = "read_parquet('" + outputDirectory + "/" + trimmed + "')" + suffix
		}

		// Check for JOIN clause (handles INNER JOIN, LEFT JOIN, RIGHT JOIN, etc.)
		if strings.HasSuffix(word, "JOIN") && i+1 < len(words) {
			tableName := words[i+1]
			// Remove trailing comma or other punctuation if present
			trimmed := strings.TrimRight(tableName, ",;)")
			suffix := tableName[len(trimmed):]
			words[i+1] = "read_parquet('" + outputDirectory + "/" + trimmed + "')" + suffix
		}
	}

	return strings.Join(words, " ")
}
