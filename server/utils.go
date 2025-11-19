package server

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
)

func getCatalogAndSchema(ctx context.Context) (string, string) {

	var catalog string
	var schema string

	for ctx != nil {
		valCtxType := reflect.TypeOf(ctx)
		if valCtxType.String() == "*context.valueCtx" {
			valField := reflect.ValueOf(ctx).Elem().FieldByName("val")

			a := fmt.Sprint(valField)
			match := regexp.MustCompile(`airport-list-flights-filter-catalog:\[([^\]]+)\]`).FindStringSubmatch(a)
			if len(match) > 1 {
				catalog = match[1]
			}

			match2 := regexp.MustCompile(`airport-list-flights-filter-schema:\[([^\]]+)\]`).FindStringSubmatch(a)
			if len(match2) > 1 {
				schema = match2[1]
			}

			ctx = reflect.ValueOf(ctx).Elem().FieldByName("Context").Interface().(context.Context)
		} else {
			break
		}
	}

	return catalog, schema
}
