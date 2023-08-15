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

func ParseQueryParams(params url.Values) (*QueryParams, error) {
	qp := &QueryParams{}

	// Parse main table (dn)
	qp.MainTable = TableNameToSQL(params.Get("dn"))

	// Parse select fields
	qp.Selects = params["select"]

	// Parse joins (links)
	for _, link := range params["link"] {
		parts := strings.Split(link, ":")
		var join JoinPart

		switch len(parts) {
		case 1:
			join = JoinPart{
				JoinType: "INNER",
				Left:     qp.MainTable + ".id",
				Right:    parts[0],
			}
		case 2:
			join = JoinPart{
				JoinType: "INNER",
				Left:     parts[0],
				Right:    parts[1],
			}
		case 3:
			join = JoinPart{
				JoinType: strings.ToUpper(parts[0]),
				Left:     parts[1],
				Right:    parts[2],
			}
		default:
			return nil, fmt.Errorf("malformed link parameter: %s", link)
		}

		qp.Joins = append(qp.Joins, join)
	}
	// Parse Filters
	for _, filter := range params["filter"] {
		parts := strings.SplitN(filter, ":", 4)
		fmt.Println(parts)
		if len(parts) < 4 {
			return nil, fmt.Errorf("malformed filter parameter: %s", filter)
		}

		fp := FilterPart{
			Operator: parts[0],
			DNPath:   parts[1] + "." + parts[2], // This was previously parts[1]
			Value:    parts[3],                  // This was previously parts[2]
		}

		qp.Filters = append(qp.Filters, fp)
	}

	/* Parse filters
	for _, filter := range params["filter"] {
		parts := strings.SplitN(filter, ":", 3)
		if len(parts) < 3 {
			return nil, fmt.Errorf("malformed filter parameter: %s", filter)
		}

		fp := FilterPart{
			Operator: parts[0],
			DNPath:   parts[1],
			Value:    parts[2],
		}

		qp.Filters = append(qp.Filters, fp)
	}
	*/

	return qp, nil
}

type JoinPart struct {
	JoinType string
	Left     string
	Right    string
}

type FilterPart struct {
	Operator string
	DNPath   string
	Value    string
}

type QueryParams struct {
	MainTable string
	Selects   []string
	Joins     []JoinPart
	Filters   []FilterPart
}

func TableNameToSQL(tableName string) string {
	parts := strings.Split(tableName, ".")
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts[:len(parts)-1], "_") + "_" + parts[len(parts)-1]
}
func ColumnNameToSQL(columnName string) string {
	parts := strings.Split(columnName, ".")
	if len(parts) < 2 {
		return columnName
	}
	tableName := strings.Join(parts[:len(parts)-1], "_")
	return tableName + "." + parts[len(parts)-1]
}

func ConstructQuery(params url.Values) (string, error) {
	qp, err := ParseQueryParams(params)
	if err != nil {
		return "", err
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
	fromClause := fmt.Sprintf("FROM %s", TableNameToSQL(qp.MainTable))

	// Building JOIN clause
	joinClauses := []string{}
	for _, join := range qp.Joins {
		joinSQL := fmt.Sprintf("%s JOIN %s ON %s = %s", join.JoinType, TableNameToSQL(strings.Split(join.Right, ".")[0]), ColumnNameToSQL(join.Left), ColumnNameToSQL(join.Right))
		joinClauses = append(joinClauses, joinSQL)
	}

	// Building WHERE clause
	whereClauses := []string{}
	for _, filter := range qp.Filters {
		filterSQL := fmt.Sprintf("%s %s '%s'", ColumnNameToSQL(filter.DNPath), filter.Operator, filter.Value)
		whereClauses = append(whereClauses, filterSQL)
	}
	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Assembling the final SQL query
	query := fmt.Sprintf("%s %s %s %s", selectClause, fromClause, strings.Join(joinClauses, " "), whereClause)

	return query, nil
}

func main() {
	//	rrawQuery := "dn=foo.bar&select=foo.bar.id&select=foo.bar.name&link=baz.qux&link=left:foo.bar.id:baz.qux.id&filter=match:foo.bar.name:John"

	rawQuery := "dn=foo.bar&select=foo.bar.id&select=foo.bar.name&link=baz.qux&link=left:foo.bar.id:baz.qux.id&filter=match:foo.bar.name:John:Wick"
	simple := "dn=domain.arp"

	queries := []string{
		rawQuery,
		simple,
		"dn=domain.arp&link=domain.arp.device_id:standard.id",
		"dn=domain.arp&link=domain.arp.device_id:standard.id&filter=match:domain.arp:ipddress:172.23.49.175/32",
	}
	expected := []string{
		`SELECT foo_bar.id, foo_bar.name FROM foo_bar INNER JOIN baz ON foo_bar.id = baz.qux LEFT JOIN baz ON foo_bar.id = baz_qux.id WHERE foo_bar.name match 'John:Wick'`,
		`SELECT * FROM domain_arp`,
		`SELECT * FROM domain_arp INNER JOIN standard ON domain_arp.device_id = standard.id`,
		`SELECT * FROM domain_arp INNER JOIN standard ON domain_arp.device_id = standard.id WHERE domain.arp.ipddress match '172.23.49.175/32'`,
	}
	for i, urlQuery := range queries {
		params, _ := url.ParseQuery(urlQuery)
		query, err := ConstructQuery(params)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println(i)
		fmt.Println(params)
		fmt.Println(urlQuery)
		fmt.Println(query)
		if query != expected[i] {
			fmt.Println(expected[i])

			fmt.Println("Failed: ", i)
		}
	}
}
