package config

import (
	"fmt"
	"io"

	"log"
	"os"

	"airportserver/sources"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	Base        Base         `toml:"base"`
	Connections []Connection `toml:"connections"`
	Sources     map[string]sources.Source
}
type Base struct {
}

type Connection struct {
	Name string
	Type string
	Data interface{}
}

func LoadConfig(configPath string) Config {
	configFile, err := os.Open(configPath)
	if err != nil {
		fmt.Printf("Error opening config.toml: %v\n", err)
		os.Exit(1)
	}
	defer configFile.Close()

	configData, err := io.ReadAll(configFile)
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		os.Exit(1)
	}

	var cfg Config
	err = toml.Unmarshal(configData, &cfg)
	if err != nil {
		panic(err)
	}

	result := map[string]sources.Source{}

	for _, conn := range cfg.Connections {
		d, err := toml.Marshal(conn.Data)
		if err != nil {
			panic(err)
		}
		s, exists := sources.GetSource(conn.Type, d)

		if exists {
			result[conn.Name] = s
		} else {
			log.Printf("Source type %s not registered", conn.Type)
		}
	}

	cfg.Sources = result
	cfg.Connections = nil

	return cfg
}
