package sources

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
)

func Tables(outputDirectory string) []string {

	result := []string{}
	err := filepath.Walk(outputDirectory+"/schemas/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}

		if !info.IsDir() {
			dpath := strings.TrimPrefix(path, outputDirectory+"/schemas/")
			result = append(result, dpath)
		}

		return nil
	})

	if err != nil {
		log.Printf("Error walking directory: %v\n", err)
	}

	return result
}

func Schema(outputDirectory string, table string) (*arrow.Schema, error) {
	f, err := os.Open(outputDirectory + "/schemas/" + table)
	if err != nil {
		return nil, errors.New("failed to Open: " + err.Error())
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		return nil, errors.New("failed to read file: " + err.Error())
	}
	return flight.DeserializeSchema(data, memory.DefaultAllocator)
}

func Preview(ctx context.Context, cancel context.CancelFunc, outputDirectory string, table string) (chan arrow.Record, error) {
	c := make(chan arrow.Record)
	go func() {
		<-ctx.Done()
		close(c)
	}()

	go func() {
		f, err := os.Open(outputDirectory + "/data/" + table)
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

			if ctx.Err() == nil {
				c <- rec
			}
			if previewRec != nil {
				previewRec.Release()
			}
			previewRec = rec
		}

		fmt.Println("Query Completed")
		cancel()
	}()
	return c, nil

}
