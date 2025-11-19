package cmd

import (
	"airportserver/config"
	"context"

	"github.com/spf13/cobra"
)

func Catalog() *cobra.Command {

	cmd := &cobra.Command{
		Use:   "catalog",
		Short: "Downloads the Flight Catalog",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			config := config.LoadConfig("config.toml")
			for _, s := range config.Sources {
				s.DownloadCatalog(context.Background())
			}
		},
	}

	return cmd
}
