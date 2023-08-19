package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	//	"log"
	"net/http"
	"strings"
	//"encoding/base64"
	"bytes"
	"unicode"

	_ "github.com/lib/pq"
)

var DB *sql.DB

var Queries = map[string]string{
	"arp":          `SELECT * FROM "domain.arp"`,
	"arp-standard": `select * from "domain.arp" as domain_arp join standard on standard.id=domain_arp.device_id`,
	"packages":     `SELECT * FROM "domain.packages"`,
	"list":         `SELECT able_name FROM information_schema.tables WHERE table_schema='public'`,
	"columns":      `SELECT column_name, data_type FROM information_schema.columns`,
	"link-tips": `WITH main_table_columns AS (
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = $1 AND table_schema = 'public'
                    )

                    SELECT
                        $1 AS main_table,
                        mtc.column_name AS main_column,
                        ic.table_name AS other_table,
                        ic.column_name AS other_column,
                        ic.data_type
                    FROM main_table_columns mtc
                    JOIN information_schema.columns ic ON mtc.data_type = ic.data_type
                    WHERE ic.table_name != $1 AND ic.table_schema = 'public'
                    ORDER BY
                        CASE WHEN ic.data_type = 'text' THEN 1 ELSE 0 END,
                        mtc.column_name,
                        ic.table_name;
                    `,
}

type RequestData struct {
	Format      string
	Query       string
	GroupByKey  string
	GroupByKey2 string
	Params      []string
}

func streamJSONGroupedDeep(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	groupedResults := make(map[string]map[string][]map[string]interface{})

	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		result := make(map[string]interface{})
		var groupKeyLevel1, groupKeyLevel2 string
		for i, colName := range cols {
			val := pointers[i].(*interface{})
			if bytes, ok := (*val).([]byte); ok {
				result[colName] = string(bytes)
			} else {
				result[colName] = *val
			}

			if colName == requestData.GroupByKey {
				groupKeyLevel1 = fmt.Sprintf("%v", result[colName])
			}
			if colName == requestData.GroupByKey2 {
				groupKeyLevel2 = fmt.Sprintf("%v", result[colName])
			}
		}

		if _, exists := groupedResults[groupKeyLevel1]; !exists {
			groupedResults[groupKeyLevel1] = make(map[string][]map[string]interface{})
		}
		groupedResults[groupKeyLevel1][groupKeyLevel2] = append(groupedResults[groupKeyLevel1][groupKeyLevel2], result)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(groupedResults); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func streamJSON(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {

	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Begin the JSON array
	w.Write([]byte("["))

	isFirst := true
	for rows.Next() {
		// Create a slice of interface{}'s to represent each column
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))

		for i := 0; i < len(cols); i++ {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		// Create map to hold the name:value pairs
		entry := make(map[string]interface{})

		for i, colName := range cols {
			var v interface{} = values[i]
			// Convert []byte type to string for JSON encoding
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			entry[colName] = v
		}

		// If it's not the first row, write a comma separator
		if isFirst {
			isFirst = false
		} else {
			w.Write([]byte(","))
		}

		if err := json.NewEncoder(w).Encode(entry); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}

	// End the JSON array
	w.Write([]byte("]"))
}

func streamCSV(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=data.csv")

	writer := csv.NewWriter(w)
	defer writer.Flush()

	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, "Failed to get columns: "+err.Error(), 500)
		return
	}

	if err := writer.Write(cols); err != nil {
		http.Error(w, "Failed to write header row: "+err.Error(), 500)
		return
	}

	columnValues := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for rows.Next() {
		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			http.Error(w, "Failed to scan row: "+err.Error(), 500)
			return
		}

		row := make([]string, len(cols))
		for i, col := range columnValues {
			switch value := col.(type) {
			case []byte:
				row[i] = string(value) // Convert []byte to string
			default:
				row[i] = fmt.Sprintf("%v", col)
			}
		}

		if err := writer.Write(row); err != nil {
			http.Error(w, "Failed to write data row: "+err.Error(), 500)
			return
		}
	}
}

func streamJSONGrouped(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	// Define the data structure for grouped results
	groupedResults := make(map[string][]map[string]interface{})

	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		result := make(map[string]interface{})
		var groupKey string
		for i, colName := range cols {
			val := pointers[i].(*interface{})
			if bytes, ok := (*val).([]byte); ok {
				result[colName] = string(bytes)
			} else {
				result[colName] = *val
			}

			// If this column is the grouping key, save its value for later use
			if colName == requestData.GroupByKey {
				groupKey = fmt.Sprintf("%v", result[colName])
			}

		}

		// Append this row's data to the appropriate group in the map
		groupedResults[groupKey] = append(groupedResults[groupKey], result)
	}

	// Convert the map to JSON and write it out
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(groupedResults); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func encodeResponse(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {

	switch requestData.Format {
	case "json":
		streamJSON(w, rows, requestData)
	case "csv":
		streamCSV(w, rows, requestData)
	case "jsonGrouped":
		if requestData.GroupByKey2 != "" {
			streamJSONGroupedDeep(w, rows, requestData)
		} else {
			streamJSONGrouped(w, rows, requestData)
		}
	default:
		http.Error(w, "Unsupported format", http.StatusBadRequest)
	}

}

func parseURL(r *http.Request) (noun string, params []string, err error) {
	path := r.URL.Path

	// Check if path ends with a slash and trim it
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	parts := strings.Split(path, "/")

	// Ensure there's at least a "/api/{noun}" structure
	if len(parts) < 3 || parts[1] != "api" {
		return "", nil, errors.New("not found")
	}

	noun = parts[2]

	// If there are additional parts, treat them as parameters
	if len(parts) > 3 {
		params = parts[3:]
	}

	return noun, params, nil
}

func parseInput(r *http.Request) (*RequestData, error) {
	noun, params, err := parseURL(r)
	if err != nil {
		return nil, err
	}

	query, exists := Queries[noun]
	if !exists {
		return nil, fmt.Errorf("Invalid API endpoint")
	}

	if len(params) != countPlaceholders(query) {
		return nil, fmt.Errorf("Endpoint %s expects %d parameters, but got %d", noun, countPlaceholders(query), params)
	}

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	groupByKey := r.URL.Query().Get("groupby")
	if format == "json" && groupByKey != "" {
		format = "jsonGrouped"
	}
	groupByKey2 := r.URL.Query().Get("groupby2")

	return &RequestData{
		Format:      format,
		Query:       query,
		GroupByKey:  groupByKey,
		GroupByKey2: groupByKey2,
		Params:      params,
	}, nil
}
func countPlaceholders(query string) int {
	found := make(map[string]struct{})
	index := 0

	for index < len(query) {
		if query[index] == '$' {
			buffer := []rune{'$'}
			index++ // Move to the next character

			// While we're still within the bounds and encountering digits
			for index < len(query) && unicode.IsDigit(rune(query[index])) {
				buffer = append(buffer, rune(query[index]))
				index++
			}

			if len(buffer) > 1 { // Exclude lone '$'
				found[string(buffer)] = struct{}{}
			}
		} else {
			index++ // Move to the next character if current is not '$'
		}
	}

	return len(found)
}

func cleanInput(reqData *RequestData) (string, []interface{}, error) {
	// Ensure the query has the correct number of placeholders
	if len(reqData.Params) != countPlaceholders(reqData.Query) {
		return "", nil, fmt.Errorf("mismatch in number of parameters and placeholders")
	}

	// Convert the []string slice to a []interface{} slice
	ifaceParams := make([]interface{}, len(reqData.Params))
	for i, v := range reqData.Params {
		ifaceParams[i] = v
	}

	// For extra safety, you can add additional sanitation logic here if needed.

	return reqData.Query, ifaceParams, nil
}

func QueryHandler(w http.ResponseWriter, r *http.Request) {
	reqData, err := parseInput(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	query, params, err := cleanInput(reqData)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	rows, err := DB.Query(query, params...)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	encodeResponse(w, rows, reqData)

}

type QueryInfo struct {
	Key       string `json:"api"` // New field for the query key
	NumParams int    `json:"num_params"`
	Example   string `json:"example"`
}

const baseURL = "http://127.0.0.1:8080/api" // Define a constant for the base URL

func generateExampleURL(key string, numParams int) string {
	if numParams == 0 {
		return fmt.Sprintf("%s/%s", baseURL, key)
	}

	// Using a buffer for performance when dealing with string manipulations
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%s/%s", baseURL, key))

	for i := 1; i <= numParams; i++ {
		buffer.WriteString(fmt.Sprintf("/{param%d}", i))
	}

	return buffer.String()
}

func QueriesHandler(w http.ResponseWriter, r *http.Request) {
	queryInfoList := make([]QueryInfo, 0, len(Queries))

	for key, query := range Queries { // Get both the key and the query
		numParams := countPlaceholders(query)
		example := generateExampleURL(key, numParams)
		queryInfoList = append(queryInfoList, QueryInfo{Key: key, NumParams: numParams, Example: example})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(queryInfoList)
}

const SERVER_HOST = ":8080"

func main() {
	var err error
	// Connect to the "testdb" database
	connStr := "user=postgres dbname=mydatabase password=mysecretpassword host=localhost sslmode=disable"
	DB, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/api/help/", QueriesHandler)
	http.HandleFunc("/api/", QueryHandler)

	fmt.Println("Server started on :8080")
	http.ListenAndServe(SERVER_HOST, nil)
}
