package sources

import (
	"context"

	"github.com/apache/arrow/go/v16/arrow"
)

type SourceInterface interface {
	DownloadCatalog(context.Context)
	Tables(context.Context) []string
	Schema(context.Context, string) (*arrow.Schema, error)
	Stream(context.Context, string) (chan arrow.Record, error)
	Preview(context.Context, string) (chan arrow.Record, error)
}

var sources = make(map[string]func([]byte) Source)

type Source struct {
	Inner SourceInterface
}

func (s Source) DownloadCatalog(ctx context.Context) {
	s.Inner.DownloadCatalog(ctx)
}

func (s Source) Tables(ctx context.Context) []string {
	return s.Inner.Tables(ctx)
}

func (s Source) Schema(ctx context.Context, query string) (*arrow.Schema, error) {
	return s.Inner.Schema(ctx, query)
}

func (s Source) Stream(ctx context.Context, query string) (chan arrow.Record, error) {
	return s.Inner.Stream(ctx, query)
}

func (s Source) Preview(ctx context.Context, query string) (chan arrow.Record, error) {
	return s.Inner.Preview(ctx, query)
}
func RegisterSource(name string, source func([]byte) Source) {
	sources[name] = source
}

func GetSource(name string, config []byte) (Source, bool) {
	inner, exists := sources[name]

	if exists {
		return inner(config), true
	}
	return Source{}, false
}
