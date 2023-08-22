package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	//"encoding/base64"
	"bytes"
	"runtime"
	"time"
	"unicode"

	_ "github.com/lib/pq"
)

var DB *sql.DB

var Queries = map[string]string{
	"arp":          `SELECT * FROM "domain.arp"`,
	"arp-standard": `select * FROM "domain.arp" as domain_arp join standard on standard.id=domain_arp.device_id`,
	"packages":     `SELECT * FROM "domain.packages"`,
	"list":         `SELECT table_name as nodes FROM information_schema.tables WHERE table_schema='public'`,
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
    "list-nodes": `
                    SELECT
                        t.table_name AS node,
                        c.column_name AS field,
                        c.data_type AS field_type,
                        coalesce(sut.n_live_tup, 0)::integer AS row_count
                    FROM
                        information_schema.tables AS t
                    JOIN
                        information_schema.columns AS c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
                    LEFT JOIN
                        pg_stat_user_tables sut ON t.table_name = sut.relname
                    WHERE
                        t.table_schema = 'public'
                    ORDER BY
                        t.table_name, c.ordinal_position;

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
func streamJSON_MEM(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Begin the JSON array
	bw := bufio.NewWriter(w)
	bw.Write([]byte("["))

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	entry := make(map[string]interface{}, len(cols))

	isFirst := true
	for rows.Next() {
		for i := 0; i < len(cols); i++ {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		for i, colName := range cols {
			var v interface{} = values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			entry[colName] = v
		}

		if isFirst {
			isFirst = false
		} else {
			bw.Write([]byte(","))
		}

		if err := json.NewEncoder(bw).Encode(entry); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}

	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	bw.Write([]byte("]"))
	bw.Flush()
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
func streamCSV2(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=data.csv")

	const bufferSize = 2 * 1024 * 1024 // 2MB
	bw := bufio.NewWriterSize(w, bufferSize)
	writer := csv.NewWriter(bw)
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

	if err = bw.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}
func streamCSV3(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=data.csv")

	const bufferSize = 2 * 1024 * 1024 // 2MB
	bw := bufio.NewWriterSize(w, bufferSize)
	writer := csv.NewWriter(bw)
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
	row := make([]string, len(cols))
	for i := range columnValues {
		columnPointers[i] = &columnValues[i]
	}

	for rows.Next() {
		if err := rows.Scan(columnPointers...); err != nil {
			http.Error(w, "Failed to scan row: "+err.Error(), 500)
			return
		}

		for i, col := range columnValues {
			switch value := col.(type) {
			case nil:
				row[i] = "<nil>"
			case []byte:
				row[i] = string(value)
			case string:
				if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
					// Remove curly braces and replace comma with ';'
					// or another character of choice for arrays
					row[i] = strings.Trim(value, "{}")
					row[i] = strings.Replace(row[i], ",", ";", -1)
				} else {
					row[i] = value
				}
			default:
				row[i] = fmt.Sprintf("%v", col)
			}
		}

		if err := writer.Write(row); err != nil {
			http.Error(w, "Failed to write data row: "+err.Error(), 500)
			return
		}
	}

	if err = bw.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
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

func streamJSONWithPQ(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	defer rows.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Begin the JSON array
	w.Write([]byte("["))

	isFirst := true
	for rows.Next() {
		var rowData string
		if err := rows.Scan(&rowData); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		// If it's not the first row, write a comma separator
		if isFirst {
			isFirst = false
		} else {
			w.Write([]byte(","))
		}

		w.Write([]byte(rowData))
	}

	// End the JSON array
	w.Write([]byte("]"))
}

func encodeResponse(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {

	switch requestData.Format {
	case "json":
		streamJSON(w, rows, requestData)
	case "jsonmem2":
		streamJSON_MEM2(w, rows, requestData)
	case "jsonpq":
		streamJSONWithPQ(w, rows, requestData)
	case "csv":
		streamCSV(w, rows, requestData)
	case "csv2":
		streamCSV2(w, rows, requestData)
	case "csv3":
		streamCSV3(w, rows, requestData)
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
func streamJSON_MEM2(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	const bufferSize = 2 * 1024 * 1024 // 2MB

	bw := bufio.NewWriterSize(w, bufferSize)
	//bw := bufio.NewWriter(w)

	if _, err = bw.Write([]byte("[")); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	entry := make(map[string]interface{}, len(cols))
	colNamesInJSON := precomputeJSONColNames(cols)

	isFirst := true
	for rows.Next() {
		for i := 0; i < len(cols); i++ {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		for i, jsonColName := range colNamesInJSON {
			var v interface{} = values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			entry[jsonColName] = v
		}

		if isFirst {
			isFirst = false
		} else {
			if _, err = bw.Write([]byte(",")); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
		}

		if err := json.NewEncoder(bw).Encode(entry); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}

	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if _, err = bw.Write([]byte("]")); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if err = bw.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

// precomputeJSONColNames computes the JSON column names in advance to avoid overhead during row processing
func precomputeJSONColNames(cols []string) []string {
	names := make([]string, len(cols))
	for i, col := range cols {
		// Convert col into its JSON representation (e.g., handle camelCasing or other transformations)
		// Here, it's a placeholder and assumes the column name does not change.
		names[i] = col
	}
	return names
}
func streamJSON3(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {
	cols, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	const bufferSize = 2 * 1024 * 1024 // 2MB
	bw := bufio.NewWriterSize(w, bufferSize)
	encoder := json.NewEncoder(bw) // Initialize the encoder here

	if _, err = bw.Write([]byte("[")); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	entry := make(map[string]interface{}, len(cols))
	colNamesInJSON := precomputeJSONColNames(cols)

	if rows.Next() {
		for i := 0; i < len(cols); i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)

		for i, jsonColName := range colNamesInJSON {
			var v interface{} = values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			entry[jsonColName] = v
		}
		if err := encoder.Encode(entry); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}

	for rows.Next() {
		if _, err = bw.Write([]byte(",")); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		for i := 0; i < len(cols); i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)

		for i, jsonColName := range colNamesInJSON {
			var v interface{} = values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			entry[jsonColName] = v
		}
		if err := encoder.Encode(entry); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}

	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if _, err = bw.Write([]byte("]")); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if err = bw.Flush(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

func LoggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		// Continue processing the request
		next(w, r)

		endTime := time.Now()
		requestDuration := endTime.Sub(startTime)

		// Log the request details
		clientIP := r.RemoteAddr
		userAgent := r.UserAgent()
		requestURI := r.RequestURI
		requestMethod := r.Method

		log.Printf("Duration: %v, IP: %s, User-Agent: %s, Method: %s, URI: %s",
			requestDuration, clientIP, userAgent, requestMethod, requestURI)
	}
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
    http.Handle("/", http.FileServer(http.Dir("../fe")))

	http.HandleFunc("/api/help/", LoggingMiddleware(QueriesHandler))
	http.HandleFunc("/api/", LoggingMiddleware(QueryHandler))

	go logMemoryUsagePeriodically()

	fmt.Println("Server started on :8080")
	http.ListenAndServe(SERVER_HOST, nil)
}
func logMemoryUsagePeriodically() {
	ticker := time.NewTicker(200 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("Alloc = %v MiB", bToMb(m.Alloc))
		log.Printf("TotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		log.Printf("Sys = %v MiB", bToMb(m.Sys))
		log.Printf("NumGC = %v\n", m.NumGC)
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
