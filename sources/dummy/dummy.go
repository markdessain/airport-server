package dummy

import (
	"context"
	"log"
	"time"

	"airportserver/sources"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/pelletier/go-toml/v2"
)

func NewDummy(config []byte) sources.Source {
	var cfg Config
	err := toml.Unmarshal(config, &cfg)
	if err != nil {
		panic(err)
	}
	return sources.Source{Inner: Dummy{config: cfg}}
}

type Config struct {
}

type Dummy struct {
	config Config
}

func (d Dummy) DownloadCatalog(ctx context.Context) {
}

func (d Dummy) Tables(ctx context.Context) []string {
	result := []string{"example_1", "example_2"}
	return result
}

func (d Dummy) Schema(ctx context.Context, table string) (*arrow.Schema, error) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	return schema, nil
}

func (d Dummy) Preview(ctx context.Context, cancel context.CancelFunc, table string) (chan arrow.Record, error) {
	c := make(chan arrow.Record)
	close(c)
	return c, nil
}

func (d Dummy) Stream(ctx context.Context, cancel context.CancelFunc, query string) (chan arrow.Record, error) {
	c := make(chan arrow.Record)
	ctx2, cancel2 := context.WithCancel(context.Background())

	channelClosed := false

	go func() {
		<-ctx.Done()
		if !channelClosed {
			cancel2()
			channelClosed = true
		}
	}()

	go func() {
		<-ctx2.Done()
		if !channelClosed {
			close(c)
			channelClosed = true
		}
	}()

	go func() {

		schema, err := d.Schema(ctx, "")
		if err != nil {
			log.Println(err)
		}

		var previewRec arrow.Record

		for _, _ = range []int{1, 2, 3, 4} {
			builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
			builder.Field(0).(*array.StringBuilder).AppendValues([]string{"a1", "a2", "a3"}, nil)

			rec := builder.NewRecord()
			rec.Retain()
			if ctx2.Err() == nil {
				c <- rec
			}
			time.Sleep(time.Millisecond * 500)

			if previewRec != nil {
				previewRec.Release()
			}
			previewRec = rec
		}

		if previewRec != nil {
			previewRec.Release()
		}

		log.Println("Query Completed")
		cancel2()

	}()

	return c, nil

}
