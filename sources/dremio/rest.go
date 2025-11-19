package dremio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	config     Config
	token      string
	baseURL    string
	httpClient *http.Client
}

type LoginRequest struct {
	UserName string `json:"userName"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token string `json:"token"`
}

type Table struct {
	Schema string `json:"TABLE_SCHEMA"`
	Name   string `json:"TABLE_NAME"`
	Type   string `json:"TABLE_TYPE"`
}

type QueryRequest struct {
	SQL string `json:"sql"`
}

type QueryResponse struct {
	Rows []map[string]interface{} `json:"rows"`
}

type JobSubmitResponse struct {
	ID string `json:"id"`
}

type JobStatusResponse struct {
	JobState     string `json:"jobState"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

type JobResultsResponse struct {
	RowCount int                      `json:"rowCount"`
	Schema   []map[string]interface{} `json:"schema"`
	Rows     []map[string]interface{} `json:"rows"`
}

type PaginatedJobResultsResponse struct {
	RowCount  int                      `json:"rowCount"`
	Schema    []map[string]interface{} `json:"schema"`
	Rows      []map[string]interface{} `json:"rows"`
	HasMore   bool                     `json:"hasMore"`
	TotalRows int                      `json:"totalRows"`
}

func NewClient(config Config) *Client {
	scheme := "http"
	if config.UseSSL {
		scheme = "https"
	}

	baseURL := fmt.Sprintf("%s://%s:%d", scheme, config.RestHost, config.RestPort)

	return &Client{
		config:  config,
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) Login() error {
	loginReq := LoginRequest{
		UserName: c.config.Username,
		Password: c.config.Password,
	}

	jsonData, err := json.Marshal(loginReq)
	if err != nil {
		return fmt.Errorf("failed to marshal login request: %v", err)
	}

	resp, err := c.httpClient.Post(
		c.baseURL+"/apiv2/login",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return fmt.Errorf("failed to login: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("login failed with status %d: %s", resp.StatusCode, string(body))
	}

	var loginResp LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return fmt.Errorf("failed to decode login response: %v", err)
	}

	c.token = loginResp.Token
	return nil
}

func (c *Client) executeQuery(sql string) (*QueryResponse, error) {
	if c.token == "" {
		return nil, fmt.Errorf("not authenticated, call Login() first")
	}

	// Step 1: Submit the query and get job ID
	jobID, err := c.submitQuery(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to submit query: %v", err)
	}

	// Step 2: Poll job status until completion
	finalStatus, errorMsg, err := c.pollJobStatus(jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to poll job status: %v", err)
	}

	// Step 3: Check if job completed successfully
	if finalStatus != "COMPLETED" {
		if errorMsg != "" {
			return nil, fmt.Errorf("job failed with status %s: %s", finalStatus, errorMsg)
		}
		return nil, fmt.Errorf("job failed with status: %s", finalStatus)
	}

	// Step 4: Fetch results
	results, err := c.fetchJobResults(jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch job results: %v", err)
	}

	return &QueryResponse{Rows: results}, nil
}

func (c *Client) submitQuery(sql string) (string, error) {
	queryReq := QueryRequest{
		SQL: sql,
	}

	jsonData, err := json.Marshal(queryReq)
	if err != nil {
		return "", fmt.Errorf("failed to marshal query request: %v", err)
	}

	req, err := http.NewRequest("POST", c.baseURL+"/api/v3/sql", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "_dremio"+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to execute query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("query submission failed with status %d: %s", resp.StatusCode, string(body))
	}

	var jobResp JobSubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		return "", fmt.Errorf("failed to decode job submission response: %v", err)
	}

	return jobResp.ID, nil
}

func (c *Client) pollJobStatus(jobID string) (string, string, error) {
	const maxTimeout = 60 * time.Second
	const pollInterval = 2 * time.Second

	timeout := time.NewTimer(maxTimeout)
	defer timeout.Stop()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeout.C:
			return "", "", fmt.Errorf("job timed out after %v", maxTimeout)
		case <-ticker.C:
			status, errorMsg, err := c.getJobStatus(jobID)
			if err != nil {
				return "", "", err
			}

			switch status {
			case "COMPLETED", "FAILED", "CANCELED":
				return status, errorMsg, nil
			case "RUNNING", "STARTING", "ENQUEUED":
				// Continue polling
				continue
			default:
				return status, errorMsg, fmt.Errorf("unknown job status: %s", status)
			}
		}
	}
}

func (c *Client) getJobStatus(jobID string) (string, string, error) {
	req, err := http.NewRequest("GET", c.baseURL+"/api/v3/job/"+jobID, nil)
	if err != nil {
		return "", "", fmt.Errorf("failed to create job status request: %v", err)
	}

	req.Header.Set("Authorization", "_dremio"+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to get job status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", "", fmt.Errorf("job status request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var statusResp JobStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		return "", "", fmt.Errorf("failed to decode job status response: %v", err)
	}

	return statusResp.JobState, statusResp.ErrorMessage, nil
}

func (c *Client) fetchJobResults(jobID string) ([]map[string]interface{}, error) {
	return c.fetchJobResultsPaginated(jobID, 0, 0)
}

func (c *Client) fetchJobResultsPaginated(jobID string, limit, offset int) ([]map[string]interface{}, error) {
	// If no limit specified, fetch all results with pagination
	if limit == 0 {
		return c.fetchAllJobResults(jobID)
	}

	return c.fetchJobResultsPage(jobID, limit, offset)
}

func (c *Client) fetchAllJobResults(jobID string) ([]map[string]interface{}, error) {
	const pageSize = 500 // Maximum allowed by Dremio API
	var allRows []map[string]interface{}
	offset := 0

	for {
		rows, err := c.fetchJobResultsPage(jobID, pageSize, offset)
		if err != nil {
			return nil, err
		}

		if len(rows) == 0 {
			break
		}

		allRows = append(allRows, rows...)

		// If we got fewer rows than the page size, we've reached the end
		if len(rows) < pageSize {
			break
		}

		offset += pageSize
	}

	return allRows, nil
}

func (c *Client) fetchJobResultsPage(jobID string, limit, offset int) ([]map[string]interface{}, error) {
	// Validate pagination parameters
	if limit > 500 {
		return nil, fmt.Errorf("limit cannot exceed 500 (Dremio API maximum)")
	}
	if limit < 0 {
		return nil, fmt.Errorf("limit cannot be negative")
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset cannot be negative")
	}

	url := fmt.Sprintf("%s/api/v3/job/%s/results", c.baseURL, jobID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create job results request: %v", err)
	}

	// Add pagination query parameters
	q := req.URL.Query()
	if limit > 0 {
		q.Add("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		q.Add("offset", strconv.Itoa(offset))
	}
	req.URL.RawQuery = q.Encode()

	req.Header.Set("Authorization", "_dremio"+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get job results: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("job results request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var resultsResp JobResultsResponse
	if err := json.NewDecoder(resp.Body).Decode(&resultsResp); err != nil {
		return nil, fmt.Errorf("failed to decode job results response: %v", err)
	}

	return resultsResp.Rows, nil
}

func (c *Client) ListTables(schema string) ([]Table, error) {
	sql := "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.\"TABLES\""

	if schema != "" {
		schema = strings.ToLower(schema)
		sql += " WHERE LOWER(TABLE_SCHEMA) LIKE '" + schema + "%'"
	}

	resp, err := c.executeQuery(sql)
	if err != nil {
		return nil, err
	}

	var tables []Table
	for _, row := range resp.Rows {
		table := Table{
			Schema: fmt.Sprintf("%v", row["TABLE_SCHEMA"]),
			Name:   fmt.Sprintf("%v", row["TABLE_NAME"]),
			Type:   fmt.Sprintf("%v", row["TABLE_TYPE"]),
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func (c *Client) TestTable(schema, tableName string) error {
	fullTableName := fmt.Sprintf(`"%s"."%s"`, schema, tableName)
	sql := fmt.Sprintf("SELECT * FROM %s LIMIT 0", fullTableName)

	_, err := c.executeQuery(sql)
	return err
}

// FetchJobResultsWithPagination fetches job results with pagination support
func (c *Client) FetchJobResultsWithPagination(jobID string, limit, offset int) (*PaginatedJobResultsResponse, error) {
	if c.token == "" {
		return nil, fmt.Errorf("not authenticated, call Login() first")
	}

	rows, err := c.fetchJobResultsPage(jobID, limit, offset)
	if err != nil {
		return nil, err
	}

	// To determine if there are more results, we check if we got a full page
	hasMore := len(rows) == limit && limit > 0

	// Get schema information from the first page if available
	var schema []map[string]interface{}
	if offset == 0 && len(rows) > 0 {
		// For offset 0, we can get schema from this request
		// For other offsets, schema would need to be cached or fetched separately
		schema = c.getJobResultsSchema(jobID)
	}

	return &PaginatedJobResultsResponse{
		RowCount:  len(rows),
		Schema:    schema,
		Rows:      rows,
		HasMore:   hasMore,
		TotalRows: -1, // Not available from Dremio API
	}, nil
}

// ExecuteQueryWithPagination executes a query and returns the first page of results
func (c *Client) ExecuteQueryWithPagination(sql string, limit, offset int) (*PaginatedJobResultsResponse, error) {
	if c.token == "" {
		return nil, fmt.Errorf("not authenticated, call Login() first")
	}

	// Submit the query and get job ID
	jobID, err := c.submitQuery(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to submit query: %v", err)
	}

	// Poll job status until completion
	finalStatus, errorMsg, err := c.pollJobStatus(jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to poll job status: %v", err)
	}

	if finalStatus != "COMPLETED" {
		if errorMsg != "" {
			return nil, fmt.Errorf("job failed with status %s: %s", finalStatus, errorMsg)
		}
		return nil, fmt.Errorf("job failed with status: %s", finalStatus)
	}

	// Fetch paginated results
	return c.FetchJobResultsWithPagination(jobID, limit, offset)
}

func (c *Client) getJobResultsSchema(jobID string) []map[string]interface{} {
	// Make a request with limit 0 to get just the schema
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v3/job/%s/results?limit=0", c.baseURL, jobID), nil)
	if err != nil {
		return nil
	}

	req.Header.Set("Authorization", "_dremio"+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var resultsResp JobResultsResponse
	if err := json.NewDecoder(resp.Body).Decode(&resultsResp); err != nil {
		return nil
	}

	return resultsResp.Schema
}
