package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
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
                        ic.table_name;`,

	"link-possible": `
            WITH specified_column AS (
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = $1 AND column_name = $2 AND table_schema = 'public'
            )
            SELECT
                ic.table_name as node,
                ic.column_name as field
            FROM
                information_schema.columns ic, specified_column sc
            WHERE
                ic.data_type = sc.data_type AND
                (ic.table_name != $1 OR ic.column_name != $2) AND
                ic.table_schema = 'public'
            ORDER BY
                ic.table_name, ic.column_name;

    `,
	"list-nodes": `SELECT
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
                        t.table_name, c.ordinal_position;`,
}

type RequestData struct {
	Format      string
	RawQuery    string
	Query       string
	GroupByKey  string
	GroupByKey2 string
	Params      []string
	Limit       string
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

// shoudl be optimized
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
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))

		for i := 0; i < len(cols); i++ {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		var entryBuilder strings.Builder
		entryBuilder.WriteString("{")

		for i, colName := range cols {
			var v interface{} = values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}

			// Escape strings and handle special JSON characters
			colNameJSON, _ := json.Marshal(colName)
			valueJSON, _ := json.Marshal(v)

			if i != 0 {
				entryBuilder.WriteString(",")
			}
			entryBuilder.WriteString(fmt.Sprintf("%s:%s", colNameJSON, valueJSON))
		}

		entryBuilder.WriteString("}")

		// If it's not the first row, write a comma separator
		if isFirst {
			isFirst = false
		} else {
			w.Write([]byte(","))
		}

		w.Write([]byte(entryBuilder.String()))
	}

	// End the JSON array
	w.Write([]byte("]"))
}

func streamJSON_no_order(w http.ResponseWriter, rows *sql.Rows, requestData *RequestData) {

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
	case "json_":
		streamJSON_no_order(w, rows, requestData)
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

func parseInputGen(r *http.Request) (*RequestData, error) {
	rawQuery := r.URL.RawQuery
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	limit := r.URL.Query().Get("limit")

	groupByKey := r.URL.Query().Get("groupby")
	if format == "json" && groupByKey != "" {
		format = "jsonGrouped"
	}
	groupByKey2 := r.URL.Query().Get("groupby2")

	reqData := &RequestData{
		Format:      format,
		RawQuery:    rawQuery,
		GroupByKey:  groupByKey,
		GroupByKey2: groupByKey2,
		Limit:       limit,
	}

	return reqData, nil
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
		return nil, fmt.Errorf("Endpoint %s expects %d parameters, but got %d", noun, countPlaceholders(query), len(params))

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
	fmt.Println(query)
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

type SMQueryOptions struct {
	TypeOperators map[string][]string `json:"type_operators"`
}

var AllowedOperators = map[string][]string{
	"text":     {"match", "notmatch", "imatch", "startswith", "istartswith", "endswith", "iendswith", "contains", "icontains"},
	"cidr":     {"match", "neq", "contained_by_or_eq", "contains_or_eq", "contained_by", "ip_contains"}, // "is_supernet_or_eq", "is_subnet_or_eq", "is_supernet", "is_subnet"},
	"inet":     {"match", "neq", "contained_by_or_eq", "contains_or_eq", "contained_by", "ip_contains"}, // "is_supernet_or_eq", "is_subnet_or_eq", "is_supernet", "is_subnet"},
	"int":      {"match", "gt", "lt", "lte", "gte", "in", "notin", "neq"},
	"uuid":     {"match", "notmatch"},
	"timezone": {"match", "notmatch", "before", "after", "on_or_before", "on_or_after", "between", "not_between", "in", "notin"},
	"array": {"array_contains", "array_is_contained", "array_overlaps", "array_match", "array_notmatch", "array_element_match", "array_concat", "array_remove_element",
		"array_has_element", "array_gt", "array_lt", "array_gte", "array_lte"},
}

func SMQueryOptionsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := SMQueryOptions{
		TypeOperators: AllowedOperators,
	}
	json.NewEncoder(w).Encode(response)
}

func cleanInputGen(reqData *RequestData) (string, []interface{}, error) {
	// Convert RawQuery back into url.Values
	urlQueryParams, err := url.ParseQuery(reqData.RawQuery)
	if err != nil {
		return "", nil, err
	}

	query, valuesMap, err := ConstructQuery(urlQueryParams)
	if err != nil {
		return "", nil, err
	}

	// Assuming valuesMap is map[string]string, but you want to convert values into a slice of interface{}
	var queryParams []interface{}
	for _, value := range valuesMap {
		queryParams = append(queryParams, value)
	}

	return query, queryParams, nil
}

func QueryGenHandler(w http.ResponseWriter, r *http.Request) {
	reqData, err := parseInputGen(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	query, params, err := cleanInputGen(reqData)
	fmt.Println("query", query)
	fmt.Println("params", params)
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
func transOperator(operator string) string {
	return SQLOperators[strings.ToLower(operator)]
}

type JoinPart struct {
	JoinType    string
	LeftTable   string
	LeftColumn  string
	RightTable  string
	RightColumn string
}

type FilterPart struct {
	Operator    string
	DNPath      string
	Value       string
	Placeholder string
}

type OrderBy struct {
	Direction string
	Field     string
}

type QueryParams struct {
	MainTable string
	Selects   []string
	Joins     []JoinPart
	Filters   []FilterPart
	Order     []OrderBy
	Limit     string
}

func splitTableAndColumn(full string) (string, string, error) {
	parts := strings.Split(full, ".")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid table.column format: %s", full)
	}
	column := parts[len(parts)-1]
	table := strings.Join(parts[:len(parts)-1], ".")
	return table, column, nil
}

func ParseQueryParams(params url.Values) (*QueryParams, error) {
	qp := &QueryParams{}
	// Parse main table (dn)
	qp.MainTable = TableNameToSQL(params.Get("dn"))
	// Parse select fields
	qp.Selects = params["field"]

	for _, link := range params["link"] {
		decodedLink, err := url.QueryUnescape(link)
		if err != nil {
			return nil, fmt.Errorf("failed to decode link parameter: %v", err)
		}

		parts := strings.Split(decodedLink, ":")
		var join JoinPart

		switch len(parts) {
		case 1:
			join = JoinPart{
				JoinType:    "INNER",
				LeftTable:   qp.MainTable,
				LeftColumn:  "id",
				RightTable:  strings.Split(parts[0], ".")[0],
				RightColumn: strings.Split(parts[0], ".")[1],
			}
		case 2:
			leftTable, leftColumn, err := splitTableAndColumn(parts[0])
			if err != nil {
				return nil, err
			}
			rightTable, rightColumn, err := splitTableAndColumn(parts[1])
			if err != nil {
				return nil, err
			}
			join = JoinPart{
				JoinType:    "INNER",
				LeftTable:   leftTable,
				LeftColumn:  leftColumn,
				RightTable:  rightTable,
				RightColumn: rightColumn,
			}
		case 3:
			joinType := strings.ToUpper(parts[0])
			leftTable, leftColumn, err := splitTableAndColumn(parts[1])
			if err != nil {
				return nil, err
			}
			rightTable, rightColumn, err := splitTableAndColumn(parts[2])
			if err != nil {
				return nil, err
			}
			join = JoinPart{
				JoinType:    joinType,
				LeftTable:   leftTable,
				LeftColumn:  leftColumn,
				RightTable:  rightTable,
				RightColumn: rightColumn,
			}
		default:
			return nil, fmt.Errorf("malformed link parameter: %s", decodedLink)
		}

		qp.Joins = append(qp.Joins, join)
	}

	// Parse Filters
	for counter, filter := range params["filter"] {
		parts := strings.SplitN(filter, ":", 3)
		fmt.Println(parts)
		if len(parts) < 3 {
			return nil, fmt.Errorf("malformed filter parameter: %s", filter)
		}

		fp := FilterPart{
			Operator:    transOperator(parts[0]),
			DNPath:      parts[1],
			Value:       parts[2],
			Placeholder: fmt.Sprintf("$%d", counter+1),
		}

		qp.Filters = append(qp.Filters, fp)
	}
	// Parse orderBy
	for _, ob := range params["orderby"] {

		parts := strings.SplitN(ob, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed orderby parameter: %s", ob)
		}

		direction := strings.ToUpper(parts[0])
		if direction != "ASC" && direction != "DESC" {
			return nil, fmt.Errorf("invalid orderby direction: %s. Only ASC or DESC is allowed", parts[0])
		}

		// Splitting the field by the last "."
		lastDotIndex := strings.LastIndex(parts[1], ".")
		fieldParts := []string{}
		if lastDotIndex != -1 {
			fieldParts = []string{parts[1][:lastDotIndex], parts[1][lastDotIndex+1:]}
			fmt.Println(fieldParts)
		} else {
			fmt.Println("Dot not found!")
		}
		if len(fieldParts) != 2 {
			return nil, fmt.Errorf("malformed orderby field: %s", parts[1])
		}
		fmt.Println("fieldparts: ", fieldParts)
		// Convert table name to its alias using the TableNameToSQL function
		tableAlias := TableNameToSQL(fieldParts[0])
		fmt.Println("TableAlis: ", tableAlias)
		// Since TableNameToSQL() adds an "AS", we split by space and take the last part as the alias
		tableAliasParts := strings.Split(tableAlias, " ")
		tableAlias = tableAliasParts[len(tableAliasParts)-1]

		field := fieldParts[1]

		orderBy := OrderBy{
			Direction: direction,
			Field:     tableAlias + "." + field, // Use the alias for the table name
		}
		fmt.Println("orderBy: ", orderBy)
		fmt.Println("TableAlis: ", tableAlias)
		fmt.Println("tableAliasParts: ", tableAliasParts)

		qp.Order = append(qp.Order, orderBy)
	}

	// Parse Limit
	qp.Limit = params.Get("limit")
	if qp.Limit != "" {
		if _, err := strconv.Atoi(qp.Limit); err != nil {
			return nil, fmt.Errorf("invalid limit value: %s", qp.Limit)
		}
	}
	return qp, nil
}

func TableNameToSQL(tableName string) string {
	parts := strings.Split(tableName, ".")
	if len(parts) == 1 {
		return parts[0]
	}
	alias := strings.Join(parts, "_")
	return fmt.Sprintf("\"%s\" AS %s", tableName, alias)
}

func ColumnNameToSQL(columnName string) string {
	parts := strings.Split(columnName, ".")
	if len(parts) < 2 {
		return columnName
	}
	tableName := strings.Join(parts[:len(parts)-1], "_")
	return tableName + "." + parts[len(parts)-1]
}

func toAlias(tableName string) string {
	return strings.ReplaceAll(tableName, ".", "_")
}

func ConstructQuery(params url.Values) (string, map[string]string, error) {
	qp, err := ParseQueryParams(params)
	if err != nil {
		return "", nil, err
	}

	// Building SELECT clause
	selectClause := "SELECT "
	if len(qp.Selects) > 0 {
		correctedSelects := make([]string, len(qp.Selects))
		for i, s := range qp.Selects {
			correctedSelects[i] = ColumnNameToSQL(s)
		}
		selectClause += strings.Join(correctedSelects, ", ")
	} else {
		selectClause += "*"
	}

	// Building FROM clause
	fromClauses := []string{}
	fromClauses = append(fromClauses, fmt.Sprintf("%s", qp.MainTable))

	// Build JOIN clause
	// INNER JOIN domain AS domain_alias ON domain_hostfile.ip_address = domain_alias.arp
	joinClauses := []string{}
	for _, join := range qp.Joins {
		leftAlias := toAlias(join.LeftTable)   // Assuming this gives you just the alias
		rightAlias := toAlias(join.RightTable) // Assuming this gives you just the alias

		joinSQL := fmt.Sprintf("%s JOIN \"%s\" AS %s ON %s.%s = %s.%s",
			join.JoinType,
			join.RightTable,
			rightAlias,
			leftAlias, join.LeftColumn,
			rightAlias, join.RightColumn)

		joinClauses = append(joinClauses, joinSQL)
	}

	// Building WHERE clause
	valuesMap := make(map[string]string)
	whereClauses := []string{}
	for _, filter := range qp.Filters {
		operator := fmt.Sprintf(filter.Operator, filter.Placeholder) // format the operator string here
		filterSQL := fmt.Sprintf("%s %s", ColumnNameToSQL(filter.DNPath), operator)
		whereClauses = append(whereClauses, filterSQL)

		valuesMap[filter.Placeholder] = filter.Value
	}
	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = "WHERE " + strings.Join(whereClauses, " AND ")
	}
	// Building orderby
	orderClause := ""
	if len(qp.Order) > 0 {
		orderParts := make([]string, len(qp.Order))
		for i, order := range qp.Order {
			orderParts[i] = fmt.Sprintf("%s %s", order.Field, order.Direction)
		}
		orderClause = "ORDER BY " + strings.Join(orderParts, ", ")
	}

	// Building limit
	limitClause := ""
	if qp.Limit != "" {
		limitClause = "LIMIT " + qp.Limit // You've already validated this as a number in the ParseQueryParams function.
	}

	// Modify the final assembling line:
	fromClause := "FROM " + strings.Join(fromClauses, ", ")
	joinClause := strings.Join(joinClauses, " ")
	query := fmt.Sprintf("%s %s %s %s %s %s", selectClause, fromClause, joinClause, whereClause, orderClause, limitClause)

	return query, valuesMap, nil
}

var SQLOperators = map[string]string{
	"match":                "= %s",
	"notmatch":             "!= %s",
	"imatch":               "ILIKE %s",
	"startswith":           "LIKE %s || '%%'",
	"istartswith":          "ILIKE %s || '%%'",
	"endswith":             "LIKE '%%' || %s",
	"iendswith":            "ILIKE '%%' || %s",
	"contains":             "LIKE '%%' || %s || '%%'",
	"icontains":            "ILIKE '%%' || %s || '%%'",
	"gt":                   "> %s",
	"lt":                   "< %s",
	"lte":                  "<= %s",
	"gte":                  ">= %s",
	"in":                   "IN (%s)",
	"notin":                "NOT IN (%s)",
	"neq":                  "<> %s",
	"contained_by_or_eq":   ">>= %s",
	"contains_or_eq":       "<<= %s",
	"contained_by":         ">> %s",
	"ip_contains":          "<< %s",
	"is_supernet_or_eq":    "~>= %s",
	"is_subnet_or_eq":      "~<= %s",
	"is_supernet":          "~> %s",
	"is_subnet":            "~< %s",
	"before":               "< %s",
	"after":                "> %s",
	"on_or_before":         "<= %s",
	"on_or_after":          ">= %s",
	"between":              "BETWEEN %s AND %s",
	"not_between":          "NOT BETWEEN %s AND %s",
	"array_contains":       " @> ARRAY[%s]",
	"array_is_contained":   "<@ ARRAY[%s]",
	"array_overlaps":       "&& ARRAY[%s]",
	"array_match":          "= ARRAY[%s]",
	"array_notmatch":       "!= ARRAY[%s]",
	"array_element_match":  "ANY(%s)",
	"array_remove_element": "- %s",
	"array_has_element":    "? %s",
	"array_gt":             "> ARRAY[%s]",
	"array_lt":             "< ARRAY[%s]",
	"array_gte":            ">= ARRAY[%s]",
	"array_lte":            "<= ARRAY[%s]",
}

func main() {
	var err error
	// Connect to the "testdb" database
	connStr := "user=postgres dbname=mydatabase password=mysecretpassword host=localhost sslmode=disable"
	DB, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	http.Handle("/", http.FileServer(http.Dir("../fe")))

	http.HandleFunc("/api/sm-query-options/", LoggingMiddleware(SMQueryOptionsHandler))

	http.HandleFunc("/api/help/", LoggingMiddleware(QueriesHandler))
	http.HandleFunc("/api/", LoggingMiddleware(QueryHandler))
	http.HandleFunc("/api/gen/", LoggingMiddleware(QueryGenHandler))

	go logMemoryUsagePeriodically()

	fmt.Println("Server started on :8080")
	http.ListenAndServe(SERVER_HOST, nil)
}
func logMemoryUsagePeriodically() {
	ticker := time.NewTicker(10 * time.Second)
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

func TableNameToAlias(tableName string) string {
	return strings.Join(strings.Split(tableName, "."), "_")
}
