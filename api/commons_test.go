package main

import (
	"strings"
	"testing"
	"unicode"
)

/*
	func countPlaceholders(query string) int {
		found := make(map[string]struct{})
		var buffer []rune
		started := false

		for _, c := range query {
			if c == '$' {
				started = true
				buffer = []rune{'$'}
				continue
			}

			if started && unicode.IsDigit(c) {
				buffer = append(buffer, c)
			} else if started {
				started = false
				if len(buffer) > 1 {  // Ensuring it's not just a lone '$'
					found[string(buffer)] = struct{}{}
				}
				buffer = nil
			}
		}

		// Handle if the query ends with a placeholder
		if started && len(buffer) > 1 {
			found[string(buffer)] = struct{}{}
		}

		return len(found)
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

*/

func TestCountPlaceholders(t *testing.T) {
	type TestCase struct {
		Input    string
		Expected int
	}

	testCases := []TestCase{
		{
			Input:    "SELECT * FROM foobar WHERE name=$1 AND hostname=$1",
			Expected: 1,
		},
		{
			Input:    "SELECT * FROM foobar WHERE name=$1 AND type=$2",
			Expected: 2,
		},
		{
			Input:    "SELECT * FROM foobar WHERE name=$1 AND type=$2 AND category=$3",
			Expected: 3,
		},
		{
			Input:    "SELECT * FROM foobar",
			Expected: 0,
		},
		{
			Input:    "SELECT * FROM foobar WHERE name=$1 AND hostname=$1",
			Expected: 1,
		},
		{
			Input:    "SELECT * FROM foobar WHERE name=$1 AND type=$2",
			Expected: 2,
		},
		{
			Input:    "SELECT * FROM foobar WHERE name=$1 AND type=$2 AND category=$3",
			Expected: 3,
		},
		{
			Input:    "SELECT * FROM foobar",
			Expected: 0,
		},
		{
			Input:    "SELECT price$ FROM products WHERE price = $1",
			Expected: 1,
		},
		{
			Input:    "SELECT * FROM items WHERE id=$10 AND type=$11",
			Expected: 2,
		},
		{
			Input:    "SELECT * FROM users WHERE username='$1abc' AND id=$1",
			Expected: 1,
		},
		{
			Input:    "",
			Expected: 0,
		},
		{
			Input:    "$",
			Expected: 0,
		},
		{
			Input:    "SELECT * FROM items WHERE id=$10 AND type=$11 AND type=$11 AND type=$11",
			Expected: 2,
		},
		{
			Input:    "SELECT * FROM products WHERE price$1=$10",
			Expected: 2,
		},
		{
			Input:    "SELECT name FROM users WHERE name ILIKE $1",
			Expected: 1,
		},
		{
			Input:    "SELECT name FROM users WHERE name iLIKE $1 OR username = $2",
			Expected: 2,
		},
		{
			Input:    strings.Repeat("$", 50), // 50 '$' characters
			Expected: 0,
		},
		{
			Input: `WITH main_table_columns AS (
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = $1 -- replace with the provided table name
                    )
                    SELECT 
                        'domain_arp' AS main_table,
                        mtc.column_name AS main_column,
                        ic.table_name AS other_table,
                        ic.column_name AS other_column,
                        ic.data_type
                    FROM main_table_columns mtc
                    JOIN information_schema.columns ic ON mtc.data_type = ic.data_type
                    WHERE ic.table_name != $2
                    ORDER BY 
                        CASE WHEN ic.data_type = $3 THEN 1 ELSE 0 END,
                        mtc.column_name, 
                        ic.table_name;`,
			Expected: 3,
		},
		{
			Input: `WITH main_table_columns AS (
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = $1 -- replace with the provided table name
                    )
                    SELECT 
                        'domain_arp' AS main_table,
                        mtc.column_name AS main_column,
                        ic.table_name AS other_table,
                        ic.column_name AS other_column,
                        ic.data_type
                    FROM main_table_columns mtc
                    JOIN information_schema.columns ic ON mtc.data_type = ic.data_type
                    WHERE ic.table_name != $2
                    ORDER BY 
                        CASE WHEN ic.data_type = $1 THEN 1 ELSE 0 END,
                        mtc.column_name, 
                        ic.table_name;`,
			Expected: 2,
		},
		{
			Input:    "SELECT * FROM products WHERE product_name LIKE $1 || '%' OR product_desc LIKE '%' || $1 || '%'",
			Expected: 1,
		},
		{
			Input:    "SELECT * FROM books WHERE title ILIKE '%' || $1 || '%' AND author ILIKE $2 || '%'",
			Expected: 2,
		},
		{
			Input:    "UPDATE users SET password = $1 WHERE username LIKE '%' || $2 || '%'",
			Expected: 2,
		},
		{
			Input:    "SELECT * FROM transactions WHERE tx_date BETWEEN $1 AND $2 AND description LIKE $3 || '%'",
			Expected: 3,
		},
		{
			Input:    "DELETE FROM logs WHERE message LIKE $1 || '%' AND severity = $2 AND timestamp < $3",
			Expected: 3,
		},
		{
			Input:    strings.Repeat("$", 50),
			Expected: 0,
		},
	}

	for testNum, testCase := range testCases {
		result := countPlaceholders(testCase.Input)
		if result != testCase.Expected {
			t.Errorf("test number %d: For input %s: Expected %d but got %d", testNum+1, testCase.Input, testCase.Expected, result)
		}
	}
}
