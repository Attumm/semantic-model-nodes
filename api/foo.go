package main

import (
	"fmt"
	"net/url"
	"strings"
)

// Convert domain name to SQL table name
func TableNameToSQL(dn string) string {
	return strings.ReplaceAll(dn, ".", "_")
}

func ConstructQuery(params url.Values) (string, error) {
	mainTable := TableNameToSQL(params.Get("dn"))
	if mainTable == "" {
		return "", fmt.Errorf("missing main table")
	}

	var selectCols []string
	selectParams, ok := params["select"]
	if !ok {
		selectCols = append(selectCols, "*")
	} else {
		for _, s := range selectParams {
			selectCols = append(selectCols, TableNameToSQL(s))
		}
	}

	// Joins (Links)
	joinStatements := []string{}
	for _, link := range params["link"] {
		parts := strings.Split(link, ":")
		var joinType, left, right string

		switch len(parts) {
		case 1: // Only table name is given (default to INNER JOIN with main table)
			joinType = "INNER"
			left = mainTable + ".id" // Assuming default join column is "id"
			right = parts[0] + ".id"
		case 2: // Table columns given without specifying join type (default to INNER JOIN)
			joinType, left, right = "INNER", parts[0], parts[1]
		case 3: // Join type and table columns are given
			joinType, left, right = strings.ToUpper(parts[0]), parts[1], parts[2]
		default:
			return "", fmt.Errorf("malformed link parameter: %s", link)
		}

		joinStatement := fmt.Sprintf("%s JOIN %s ON %s = %s", joinType, TableNameToSQL(strings.Split(right, ".")[0]), left, right)
		joinStatements = append(joinStatements, joinStatement)
	}

	// Filters (Where clauses)
	whereStatements := []string{}
	for _, filter := range params["filter"] {
		parts := strings.Split(filter, ":")
		if len(parts) != 3 {
			return "", fmt.Errorf("malformed filter parameter: %s", filter)
		}

		operator, column, value := parts[0], parts[1], parts[2]
		whereStatement := ""
		switch operator {
		case "match":
			whereStatement = fmt.Sprintf("%s = '%s'", column, value)
			// Add other operators as needed
		}
		whereStatements = append(whereStatements, whereStatement)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectCols, ", "), mainTable)
	if len(joinStatements) > 0 {
		query += " " + strings.Join(joinStatements, " ")
	}
	if len(whereStatements) > 0 {
		query += " WHERE " + strings.Join(whereStatements, " AND ")
	}
	return query, nil
}

func main() {
	rawQuery := "dn=foo.bar&select=foo.bar.id&select=foo.bar.name&link=baz.qux&link=left:foo.bar.id:baz.qux.id&filter=match:foo.bar.name:John"
	params, _ := url.ParseQuery(rawQuery)
	query, err := ConstructQuery(params)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println(query)
}
