package cmd

import (
	"context"

	"log"
	"os"
	"os/signal"

	"airportserver/config"
	"airportserver/server"

	"github.com/spf13/cobra"
)

func Server() *cobra.Command {
	return &cobra.Command{
		Use:   "server",
		Short: "Starts the Flight Server",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			defer func() {
				signal.Stop(c)
				cancel()
			}()

			go func() {
				select {
				case <-c:
					log.Println("Shutting down ...")
					cancel()
				case <-ctx.Done():
				}
			}()

			config := config.LoadConfig("config.toml")

			// go server.Caddy()
			go server.Launch(ctx, config)

			select {}
		},
	}
}
