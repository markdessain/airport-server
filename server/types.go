package server

import "time"

type Content struct {
	SHA256     string  `json:"sha256" msgpack:"sha256"`
	URL        *string `json:"url" msgpack:"url"`
	Serialized *string `json:"serialized" msgpack:"serialized"`
}

type Schema struct {
	Name        string            `json:"name" msgpack:"name"`
	Description string            `json:"description" msgpack:"description"`
	Tags        map[string]string `json:"tags" msgpack:"tags"`
	Content     Content           `json:"content" msgpack:"content"`
	IsDefault   bool              `json:"is_default" msgpack:"is_default"`
}

type VersionInfo struct {
	CatalogVersion int  `json:"catalog_version" msgpack:"catalog_version"`
	IsFixed        bool `json:"is_fixed" msgpack:"is_fixed"`
}

type FlightData struct {
	Content     Content     `json:"content" msgpack:"content"`
	Schemas     []Schema    `json:"schemas" msgpack:"schemas"`
	VersionInfo VersionInfo `json:"version_info" msgpack:"version_info"`
}

type AppMetadata struct {
	Type       string                 `json:"type" msgpack:"type"`
	Catalog    string                 `json:"catalog" msgpack:"catalog"`
	Schema     string                 `json:"schema" msgpack:"schema"`
	Name       string                 `json:"name" msgpack:"name"`
	Comment    string                 `json:"comment" msgpack:"comment"`
	ActionName string                 `json:"action_name" msgpack:"action_name"`
	ExtraData  map[string]interface{} `json:"extra_data" msgpack:"extra_data"`
}

type Transaction struct {
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"created_at"`
	Status     string    `json:"status"`
	Operations []string  `json:"operations,omitempty"`
}

type TicketData struct {
	FlightName  string `json:"flight_name" msgpack:"flight_name"`
	JsonFilters string `json:"json_filters" msgpack:"json_filters"`
	ColumnIds   []int  `json:"column_ids" msgpack:"column_ids"`
}

type Catalog struct {
	CatalogName string `json:"catalog_name" msgpack:"extra_data"`
}

type Ticket struct {
	Descriptor string `msgpack:"descriptor"`
}
