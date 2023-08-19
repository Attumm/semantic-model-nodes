package main

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// Test structure for our unit tests
type TestCase struct {
	Input    string // the raw query string for URL parameters
	Expected string // the expected WHERE clause
	Err      error  // expected error if any
}

func TestConstructWhereClause(t *testing.T) {
	mainTable := "main_table_name" // This can be set to a default table or passed as an argument

	testCases := []TestCase{
		// 1 Happy path tests
		{
			Input:    "match-column=value",
			Expected: "WHERE main_table_name.column = $1",
			Err:      nil,
		},
		// 2 Happy path tests
		{
			Input:    "table1-match-column=value",
			Expected: "WHERE table1.column = $1",
			Err:      nil,
		},
		// 3 Happy path tests
		{
			Input:    "match-column1=value1&match-column2=value2",
			Expected: "WHERE main_table_name.column1 = $1 AND main_table_name.column2 = $2",
			Err:      nil,
		},
		// 4 Boundary tests
		{
			Input:    "", // No parameters
			Expected: "",
			Err:      nil,
		},
		// 5 Boundary tests
		{
			Input:    "nonexistent-column=value", // Invalid column name
			Expected: "",
			Err:      fmt.Errorf("invalid column name: nonexistent-column"),
		},
		// 6 Test case: Column with "startswith" operator
		{
			Input:    "startswith-column=valu",
			Expected: "WHERE main_table_name.column LIKE $1",
			Err:      nil,
		},
		// 7 Test case: Column with "endswith" operator
		{
			Input:    "endswith-column=ue",
			Expected: "WHERE main_table_name.column LIKE $1",
			Err:      nil,
		},
		// 8 Test case: Column with "contains" operator
		{
			Input:    "contains-column=alu",
			Expected: "WHERE main_table_name.column LIKE $1",
			Err:      nil,
		},
		// 9 Test case: Column with invalid operator
		{
			Input:    "invalidOp-column=value",
			Expected: "",
			Err:      fmt.Errorf("invalid operator invalidOp for column column"),
		},
		// 10 Test case: CIDR type with various operators
		{
			Input:    "contains-or-eq-cidr=192.168.1.0/24",
			Expected: "WHERE main_table_name.cidr <<= $1",
			Err:      nil,
		},
		// 11 Test case: CIDR type with various operators
		{
			Input:    "is_supernet_or_eq-cidr=192.168.1.0/24",
			Expected: "WHERE main_table_name.cidr ~>= $1",
			Err:      nil,
		},
		// 12 Test case: Using both text type and cidr type columns in the query
		{
			Input:    "startswith-column=valu&contains-or-eq-cidr=192.168.1.0/24",
			Expected: "WHERE main_table_name.column LIKE $1 AND main_table_name.cidr <<= $2",
			Err:      nil,
		},
		// 13 Test case: Query without a dash separator between operator and column
		{
			Input:    "startswithcolumn=valu",
			Expected: "",
			Err:      fmt.Errorf("malformed query parameter key: startswithcolumn"),
		},
		// 14 Boundary case: Empty WHERE clauses
		{
			Input:    "invalidOp-nonexistent=value",
			Expected: "",
			Err:      fmt.Errorf("invalid column name: nonexistent"),
		},
		// 15 Boundary case: Valid keys but all are invalid in terms of our map, ensuring an empty WHERE clause
		{
			Input:    "invalidOp-column=value&anotherInvalidOp-anotherColumn=value2",
			Expected: "",
			Err:      nil,
		},
	}

	for testNum, testCase := range testCases {
		// Parse the raw query string
		values, err := url.ParseQuery(testCase.Input)
		if err != nil {
			t.Errorf("test number: %d Failed to parse query: %v", testNum+1, err)
		}

		// Get the result from the function
		result, _, err := ConstructWhereClause(mainTable, values)

		// Check for expected errors
		if testCase.Err != nil && err == nil {
			t.Errorf("test number: %d Expected error but got none for input: %s", testNum+1, testCase.Input)
			continue
		} else if testCase.Err == nil && err != nil {
			t.Errorf("test number: %d Unexpected error for input %s: %v", testNum+1, testCase.Input, err)
			continue
		} else if testCase.Err != nil && err != nil && testCase.Err.Error() != err.Error() {
			t.Errorf("test number: %d Expected error: %v, but got: %v", testNum+1, testCase.Err, err)
			continue
		}

		// Compare the result with the expected output
		if result != testCase.Expected {
			t.Errorf("test number: %d Mismatch! Input: %s, Expected: %s, Got: %s", testNum+1, testCase.Input, testCase.Expected, result)
		}
	}

}
func TestConstructWhereClauseSecurity(t *testing.T) {

	testCases := []TestCase{
		// 1 SQL Injection attempt using text type
		{
			Input:    "match-column=';DROP TABLE main_table_name;--",
			Expected: "WHERE main_table_name.column = $1",
			Err:      nil,
		},
		// 2 SQL Injection attempt using CIDR type
		{
			Input:    "match-cidr=';DROP TABLE main_table_name;--",
			Expected: "WHERE main_table_name.cidr = $1",
			Err:      nil,
		},
		// 3 Excessive input
		{
			Input:    "match-column=" + strings.Repeat("a", 10000),
			Expected: "WHERE main_table_name.column = $1",
			Err:      nil,
		},
		// 4 Using special/edge characters
		{
			Input:    "match-column=" + url.QueryEscape("`~!@#$%^&*()_-+={}[]|\\:;\"'<>,.?/"),
			Expected: "WHERE main_table_name.column = $1",
			Err:      nil,
		},
		// 5 Combination of valid and malicious input
		{
			Input:    "startswith-column=valid&match-column=';DROP TABLE main_table_name;--",
			Expected: "WHERE main_table_name.column LIKE $1 AND main_table_name.column = $2",
			Err:      nil,
		},
	}
	for testNum, testCase := range testCases {
		values, err := url.ParseQuery(testCase.Input)
		if err != nil {
			t.Fatalf("test number %d: Failed to parse query for input %s: %v", testNum+1, testCase.Input, err)
		}

		mainTable := "main_table_name"
		result, _, err := ConstructWhereClause(mainTable, values)

		// Checking result first
		if result != testCase.Expected {
			t.Errorf("test number %d: Mismatch! Input: %s, Expected: %s, Got: %s", testNum+1, testCase.Input, testCase.Expected, result)
		}

		// Then check the error conditions
		if testCase.Err != nil {
			if err == nil {
				t.Errorf("test number %d: Expected error but got none for input: %s", testNum+1, testCase.Input)
			} else if testCase.Err.Error() != err.Error() {
				t.Errorf("test number %d: Expected error: %v, but got: %v for input: %s", testNum+1, testCase.Err, err, testCase.Input)
			}
		} else if err != nil {
			t.Errorf("test number %d: Unexpected error for input %s: %v", testNum+1, testCase.Input, err)
		}
	}

}

func TestConstructSelectWithJoin(t *testing.T) {
	testCases := []struct {
		name          string
		params        url.Values
		expectedQuery string
		expectedErr   bool
	}{
		{
			name: "1 Basic Select with No Joins",
			params: url.Values{
				"dn": []string{"domain.arp"},
			},
			expectedQuery: "SELECT * FROM domain_arp ",
			expectedErr:   false,
		},
		{
			name: "2 Select with Single Join",
			params: url.Values{
				"dn":   []string{"domain.arp"},
				"join": []string{"users.id=orders.userId"},
			},
			expectedQuery: "SELECT * FROM domain_arp JOIN orders ON users.id = orders.userId ",
			expectedErr:   false,
		},
		{
			name: "3 Select with Multiple Joins",
			params: url.Values{
				"dn":   []string{"domain.arp"},
				"join": []string{"users.id=orders.userId", "orders.orderId=shipments.id"},
			},
			expectedQuery: "SELECT * FROM domain_arp JOIN orders ON users.id = orders.userId JOIN shipments ON orders.orderId = shipments.id ",
			expectedErr:   false,
		},
		{
			name: "4 Malformed Join",
			params: url.Values{
				"dn":   []string{"domain.arp"},
				"join": []string{"users.idorders.userId"},
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, _, err := ConstructSelectWithJoin(tc.params)
			if (err != nil) != tc.expectedErr {
				t.Errorf("%s got error %v, expected error %v", tc.name, err, tc.expectedErr)
			}
			if query != tc.expectedQuery {
				t.Errorf("%s  got query %q, want %q", tc.name, query, tc.expectedQuery)
			}
		})
	}
}
