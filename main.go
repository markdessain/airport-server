package main

import (
	"airportserver/cmd"
	"airportserver/sources"
	"airportserver/sources/dremio"
	"airportserver/sources/dummy"
	"fmt"
	"os"
)

func main() {
	sources.RegisterSource("dremio", dremio.NewDremio)
	sources.RegisterSource("dummy", dummy.NewDummy)

	if err := cmd.Root().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
