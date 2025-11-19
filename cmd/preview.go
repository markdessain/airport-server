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
	var query string

	cmd := &cobra.Command{
		Use:   "preview",
		Short: "Downloads the Flight Catalog",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			config := config.LoadConfig("config.toml")

			for name, s := range config.Sources {
				if name == source {
					defer s.Cleanup(context.Background())
					schema, err := s.Schema(context.Background(), query)

					if err != nil {
						fmt.Println(err)
					}
					fmt.Println(schema)

					stream, err := s.Stream(context.Background(), "SELECT * FROM ("+query+") LIMIT 5")
					if err != nil {
						fmt.Println(err)
					}

					for record := range stream {
						var buf bytes.Buffer
						enc := json.NewEncoder(&buf)
						enc.SetIndent("", "  ")
						if err := enc.Encode(record); err != nil {
							panic(err)
						}

						fmt.Println(buf.String())
					}
				}
			}

		},
	}
	cmd.Flags().StringVarP(&source, "source", "s", "", "source")
	cmd.Flags().StringVarP(&query, "query", "q", "", "query")

	return cmd
}
