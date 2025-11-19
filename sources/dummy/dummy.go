package dummy

import (
	"context"

	"airportserver/sources"

	"github.com/apache/arrow/go/v16/arrow"
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
			{Name: "b", Type: arrow.BinaryTypes.String},
			{Name: "c", Type: arrow.BinaryTypes.String},
			{Name: "d", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	return schema, nil
}

func (d Dummy) Preview(ctx context.Context, table string) (chan arrow.Record, error) {
	c := make(chan arrow.Record)
	close(c)
	return c, nil
}

func (d Dummy) Stream(ctx context.Context, query string) (chan arrow.Record, error) {
	c := make(chan arrow.Record)
	close(c)
	return c, nil
}
