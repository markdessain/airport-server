package main

import (
	"airportserver/cmd"
	"airportserver/sources"
	"airportserver/sources/dremio"
	"fmt"
	"os"
)

func main() {
	sources.RegisterSource("dremio", dremio.NewDremio)

	if err := cmd.Root().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
