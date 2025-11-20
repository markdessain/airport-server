package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"airportserver/config"

	"github.com/spf13/cobra"
)

func Preview() *cobra.Command {
	var source string
	var table string

	cmd := &cobra.Command{
		Use:   "preview",
		Short: "Downloads the Flight Catalog",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			config := config.LoadConfig("config.toml")

			for name, s := range config.Sources {
				if name == source {
					schema, err := s.Schema(context.Background(), table)

					if err != nil {
						fmt.Println(err)
					}
					fmt.Println(schema)

					requestCtx, requestCancel := context.WithCancel(context.Background())

					stream, err := s.Preview(requestCtx, requestCancel, table)
					if err != nil {
						fmt.Println(err)
					}

					i := 0

					for record := range stream {
						i++
						var buf bytes.Buffer
						enc := json.NewEncoder(&buf)
						enc.SetIndent("", "  ")
						if err := enc.Encode(record); err != nil {
							panic(err)
						}

						fmt.Println(buf.String())

						if i > 2 {
							requestCancel()
						}
					}
				}
			}

		},
	}
	cmd.Flags().StringVarP(&source, "source", "s", "", "source")
	cmd.Flags().StringVarP(&table, "table", "t", "", "table")

	return cmd
}
