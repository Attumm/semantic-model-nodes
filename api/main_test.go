package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	_ "github.com/lib/pq"
)

var EXPECTEDDATA = []map[string]interface{}{
	{"id": 1, "name": "John"},
	{"id": 2, "name": "Jane"},
	{"id": 3, "name": "Alice"},
	{"id": 4, "name": "Bob"},
	{"id": 5, "name": "Charlie"},
}

func init() {
	var err error
	// Connect to the "testdb" database
	connStr := "user=postgres dbname=mydatabase password=mysecretpassword host=localhost sslmode=disable"
	DB, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	// Setup our test table and insert some data
	setupQuery := `
    CREATE TABLE IF NOT EXISTS test (
        id INT PRIMARY KEY,
        name TEXT NOT NULL
    );
    DELETE FROM test;
`

	inserts := ""
	for _, data := range EXPECTEDDATA {
		inserts += fmt.Sprintf("INSERT INTO test (id, name) VALUES (%v, '%s');\n", data["id"], data["name"])
	}
	//	inserts := ""
	//	for _, data := range EXPECTEDDATA {
	//inserts += fmt.Sprintf("INSERT INTO test (id, name) VALUES ('%v', '%s');\n", data["id"], data["name"])

	// }

	_, err = DB.Exec(setupQuery + inserts)
	if err != nil {
		panic(err)
	}
	Queries["test"] = "SELECT id, name FROM test ORDER BY id ASC"
}

func TestApiHandlerCSV(t *testing.T) {
	// Create a request to pass to our handler
	req, err := http.NewRequest("GET", "/api/test?format=csv", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create a ResponseRecorder to record the response
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ApiHandler)

	handler.ServeHTTP(rr, req)

	// Check the status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	// Check the response body
	reader := csv.NewReader(strings.NewReader(rr.Body.String()))

	// Check the CSV header
	header, err := reader.Read()
	if err != nil {
		t.Fatal("Failed to read CSV header:", err)
	}
	if !reflect.DeepEqual(header, []string{"id", "name"}) {
		t.Errorf("handler returned unexpected header: got %v want %v", header, []string{"id", "name"})
	}

	for _, expectedRow := range EXPECTEDDATA {
		record, err := reader.Read()
		if err != nil {
			t.Fatal("Failed to read CSV:", err)
		}
		idInt, ok1 := expectedRow["id"].(int)
		id := strconv.Itoa(idInt)
		name, ok2 := expectedRow["name"].(string)

		if !ok1 || !ok2 {
			t.Errorf("Unexpected type for id or name")
		} else if !reflect.DeepEqual(record, []string{id, name}) {
			t.Errorf("handler returned unexpected data: got %v want %v", record, []string{id, name})
		}

	}
}
func constructExpectedString(data []map[string]interface{}) string {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return ""
	}
	return string(jsonData)
}

func cleanString(s string) string {
	return strings.Join(strings.Fields(s), "")
}

func TestApiHandlerJSON(t *testing.T) {
	// Create a request to pass to our handler
	req, err := http.NewRequest("GET", "/api/test?format=json", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create a ResponseRecorder to record the response
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ApiHandler)

	handler.ServeHTTP(rr, req)

	// Check the status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	//fmt.Println(string(rr.Body.Bytes()))
	returnedStr := cleanString(string(rr.Body.Bytes()))
	expectedStr := cleanString(constructExpectedString(EXPECTEDDATA))

	if returnedStr != expectedStr {
		t.Errorf("handler returned unexpected data: got %v want %v", returnedStr, expectedStr)
	}
}
