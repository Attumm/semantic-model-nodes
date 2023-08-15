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

	_ "github.com/lib/pq"
)

var DB *sql.DB

var Queries = map[string]string{
	"arp":          `SELECT * FROM domain_arp`,
	"arp-standard": `select * from domain_arp join standard on standard.id=domain_arp.device_id`,
	"packages":     `SELECT * FROM domain_packages`,
	"list":         `SELECT table_name FROM information_schema.tables WHERE table_schema='public'`,
	"columns":      `SELECT column_name, data_type FROM information_schema.columns`,
}

type RequestData struct {
	Format      string
	Query       string
	GroupByKey  string
	GroupByKey2 string
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

func parseNounFromURL(r *http.Request) (string, error) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 || parts[1] != "api" {
		return "", errors.New("not found")
	}
	return parts[2], nil
}

func handleInput(r *http.Request) (*RequestData, error) {
	noun, err := parseNounFromURL(r)
	if err != nil {
		return nil, err
	}

	query, exists := Queries[noun]
	if !exists {
		return nil, fmt.Errorf("Invalid API endpoint")
	}

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	groupByKey := r.URL.Query().Get("groupbykey")
	if format == "json" && groupByKey != "" {
		format = "jsonGrouped"
	}
	groupByKey2 := r.URL.Query().Get("groupbykey2")

	return &RequestData{
		Format:      format,
		Query:       query,
		GroupByKey:  groupByKey,
		GroupByKey2: groupByKey2,
	}, nil

}

func ApiHandler(w http.ResponseWriter, r *http.Request) {
	reqData, err := handleInput(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	safeQuery := reqData.Query //TODO  Make safe

	rows, err := DB.Query(safeQuery)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	encodeResponse(w, rows, reqData)

}

func main() {
	var err error
	// Connect to the "testdb" database
	connStr := "user=postgres dbname=mydatabase password=mysecretpassword host=localhost sslmode=disable"
	DB, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/api/", ApiHandler)

	fmt.Println("Server started on :8080")
	http.ListenAndServe(":8080", nil)
}
