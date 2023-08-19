package main

import (
	"fmt"
	"net/url"
	"strings"
)

type OperatorMapping struct {
	QueryParam  string
	SQLOperator string
}

var typeOperatorsMap = map[string][]OperatorMapping{
	"text": {
		{QueryParam: "match", SQLOperator: "%s = $%s"},                         // Equals
		{QueryParam: "not_match", SQLOperator: "%s != $%s"},                    // Not Equals
		{QueryParam: "imatch", SQLOperator: "%s ILIKE $%s"},                    // Case-Insensitive Equals
		{QueryParam: "startswith", SQLOperator: "%s LIKE $%s || '%%'"},         // Starts With
		{QueryParam: "istartswith", SQLOperator: "%s ILIKE $%s || '%%'"},       // Case-Insensitive Starts With
		{QueryParam: "endswith", SQLOperator: "%s LIKE '%%' || $%s"},           // Ends With
		{QueryParam: "iendswith", SQLOperator: "%s ILIKE '%%' || $%s"},         // Case-Insensitive Ends With
		{QueryParam: "contains", SQLOperator: "%s LIKE '%%' || $%s || '%%'"},   // Contains
		{QueryParam: "icontains", SQLOperator: "%s ILIKE '%%' || $%s || '%%'"}, // Case-Insensitive Contains
	},
	"cidr": {
		{QueryParam: "match", SQLOperator: "%s = $%s"},
		{QueryParam: "neq", SQLOperator: "%s <> $%s"},
		{QueryParam: "contained_by_or_eq", SQLOperator: "%s >>= $%s"},
		{QueryParam: "contains_or_eq", SQLOperator: "%s <<= $%s"},
		{QueryParam: "contained_by", SQLOperator: "%s >> $%s"},
		{QueryParam: "contains", SQLOperator: "%s << $%s"},
		{QueryParam: "is_supernet_or_eq", SQLOperator: "%s ~>= $%s"},
		{QueryParam: "is_subnet_or_eq", SQLOperator: "%s ~<= $%s"},
		{QueryParam: "is_supernet", SQLOperator: "%s ~> $%s"},
		{QueryParam: "is_subnet", SQLOperator: "%s ~< $%s"},
	},
	"int": {
		{QueryParam: "match", SQLOperator: "%s = $%s"},
		{QueryParam: "eq", SQLOperator: "%s = $%s"},
		{QueryParam: "ne", SQLOperator: "%s != $%s"},
		{QueryParam: "gt", SQLOperator: "%s > $%s"},
		{QueryParam: "lt", SQLOperator: "%s < $%s"},
		{QueryParam: "gte", SQLOperator: "%s >= $%s"},
		{QueryParam: "lte", SQLOperator: "%s <= $%s"},
		{QueryParam: "in", SQLOperator: "%s IN ($%s)"},
		{QueryParam: "nin", SQLOperator: "%s NOT IN ($%s)"},
	},
	"uuid": {
		{QueryParam: "eq", SQLOperator: "%s = $%s"},   // Equals
		{QueryParam: "neq", SQLOperator: "%s != $%s"}, // Not Equals
	},
	"timezone": {
		{QueryParam: "eq", SQLOperator: "%s = $%s"},                            // Equals
		{QueryParam: "neq", SQLOperator: "%s != $%s"},                          // Not Equals
		{QueryParam: "before", SQLOperator: "%s < $%s"},                        // Before
		{QueryParam: "after", SQLOperator: "%s > $%s"},                         // After
		{QueryParam: "on_or_before", SQLOperator: "%s <= $%s"},                 // On or Before
		{QueryParam: "on_or_after", SQLOperator: "%s >= $%s"},                  // On or After
		{QueryParam: "between", SQLOperator: "%s BETWEEN $%s AND $%s"},         // Between
		{QueryParam: "not_between", SQLOperator: "%s NOT BETWEEN $%s AND $%s"}, // Not Between
		{QueryParam: "in", SQLOperator: "%s IN ($%s)"},                         // In
		{QueryParam: "nin", SQLOperator: "%s NOT IN ($%s)"},                    // Not In
	},
}

func ConstructWhereClause(mainTable string, params url.Values) (string, []interface{}, error) {
	whereClauses := []string{}
	values := []interface{}{}
	paramCount := 1

	for key, valList := range params {
		parts := strings.SplitN(key, "-", 2)
		if len(parts) != 2 {
			return "", nil, fmt.Errorf("malformed query parameter key: %s", key)
		}

		operator, columnAndTable := parts[0], parts[1]
		tableParts := strings.SplitN(columnAndTable, ".", 2)

		var column, table string
		if len(tableParts) == 2 {
			table, column = tableParts[0], tableParts[1]
		} else {
			column = tableParts[0]
			table = mainTable
		}

		if ops, ok := typeOperatorsMap[column]; ok {
			var sqlOperator string
			for _, op := range ops {
				if op.QueryParam == operator {
					sqlOperator = op.SQLOperator
					break
				}
			}
			if sqlOperator == "" {
				return "", nil, fmt.Errorf("invalid operator %s for column %s", operator, column)
			}

			whereClause := fmt.Sprintf(sqlOperator, table+"."+column, fmt.Sprintf("$%d", paramCount))
			whereClauses = append(whereClauses, whereClause)
			values = append(values, valList[0])
			paramCount++
		} else {
			return "", nil, fmt.Errorf("invalid column name: %s", column)
		}
	}

	return "WHERE " + strings.Join(whereClauses, " AND "), values, nil
}

func TableNameToSQL(tableName string) string {
	return strings.ReplaceAll(tableName, ".", "_")
}

func ConstructSelectWithJoin(params url.Values) (string, []interface{}, error) {
	mainTable := TableNameToSQL(params.Get("dn"))
	if mainTable == "" {
		return "", nil, fmt.Errorf("missing main table")
	}

	joins, joinOK := params["join"]
	whereClause, values, err := ConstructWhereClause(mainTable, params)
	if err != nil {
		return "", nil, err
	}

	selectStmt := fmt.Sprintf("SELECT * FROM %s ", mainTable)
	if joinOK {
		for _, join := range joins {
			parts := strings.SplitN(join, "=", 2)
			if len(parts) != 2 {
				return "", nil, fmt.Errorf("malformed join: %s", join)
			}

			leftParts := strings.SplitN(parts[0], ".", 2)
			rightParts := strings.SplitN(parts[1], ".", 2)
			if len(leftParts) != 2 || len(rightParts) != 2 {
				return "", nil, fmt.Errorf("malformed join fields: %s", join)
			}

			// Convert "domain.table" format to "domain_table"
			leftTable := TableNameToSQL(leftParts[0])
			rightTable := TableNameToSQL(rightParts[0])
			leftColumn := leftParts[1]
			rightColumn := rightParts[1]

			joinStmt := fmt.Sprintf("JOIN %s ON %s.%s = %s.%s ", rightTable, leftTable, leftColumn, rightTable, rightColumn)
			selectStmt += joinStmt
		}
	}

	selectStmt += whereClause
	return selectStmt, values, nil
}
