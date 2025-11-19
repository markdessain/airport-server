package cmd

import (
	"github.com/spf13/cobra"
)

func Root() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dremio-schemas",
		Short: "A CLI tool to interact with Dremio and manage schemas",
		Long:  `A command line tool that helps describe what tables exist in your Dremio instance.`,
	}
	cmd.AddCommand(Server())
	cmd.AddCommand(Catalog())
	cmd.AddCommand(Preview())

	return cmd
}
