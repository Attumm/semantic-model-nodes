package main

import (
	"fmt"
	"net/url"
	"strings"
)

// 1. Operator Map
var SQLOperators = map[string]string{
	"MATCH":              "= %s",
	"NOTMATCH":           "!= %s",
	"IMATCH":             "ILIKE %s",
	"STARTSWITH":         "LIKE %s || '%%'",
	"ISTARTSWITH":        "ILIKE %s || '%%'",
	"ENDSWITH":           "LIKE '%%' || %s",
	"IENDSWITH":          "ILIKE '%%' || %s",
	"TEXT_CONTAINS":      "LIKE '%%' || %s || '%%'",
	"ITEXT_CONTAINS":     "ILIKE '%%' || %s || '%%'",
	"GT":                 "> %s",
	"LT":                 "< %s",
	"LTE":                "<= %s",
	"GTE":                ">= %s",
	"IN":                 "IN (%s)",
	"NOTIN":              "NOT IN (%s)",
	"NEQ":                "<> %s",
	"CONTAINED_BY_OR_EQ": ">>= %s",
	"CONTAINS_OR_EQ":     "<<= %s",
	"CONTAINED_BY":       ">> %s",
	"CONTAINS":           "<< %s",
	"IS_SUPERNET_OR_EQ":  "~>= %s",
	"IS_SUBNET_OR_EQ":    "~<= %s",
	"IS_SUPERNET":        "~> %s",
	"IS_SUBNET":          "~< %s",
	"BEFORE":             "< %s",
	"AFTER":              "> %s",
	"ON_OR_BEFORE":       "<= %s",
	"ON_OR_AFTER":        ">= %s",
	"BETWEEN":            "BETWEEN %s AND %s",
	"NOT_BETWEEN":        "NOT BETWEEN %s AND %s",
	// ... add others as needed
}

// 2. Allowed Operator Map
var AllowedOperators = map[string][]string{
	"text":     {"MATCH", "NOTMATCH", "IMATCH", "STARTSWITH", "ISTARTSWITH", "ENDSWITH", "IENDSWITH", "CONTAINS", "ICONTAINS"},
	"cidr":     {"MATCH", "NEQ", "CONTAINED_BY_OR_EQ", "CONTAINS_OR_EQ", "CONTAINED_BY", "CONTAINS", "IS_SUPERNET_OR_EQ", "IS_SUBNET_OR_EQ", "IS_SUPERNET", "IS_SUBNET"},
	"int":      {"MATCH", "GT", "LT", "LTE", "GTE", "IN", "NOTIN", "NEQ"},
	"uuid":     {"MATCH", "NOTMATCH"},
	"timezone": {"MATCH", "NOTMATCH", "BEFORE", "AFTER", "ON_OR_BEFORE", "ON_OR_AFTER", "BETWEEN", "NOT_BETWEEN", "IN", "NOTIN"},
	// ... add others as needed
}

func isValidOperatorForType(dataType, operatorKey string) bool {
	for _, validOp := range AllowedOperators[dataType] {
		if validOp == operatorKey {
			return true
		}
	}
	return false
}

func transOperator(operator string) string {
	return SQLOperators[strings.ToUpper(operator)]
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
	return qp, nil
}

type JoinPart struct {
	JoinType string
	Left     string
	Right    string
}

type FilterPart struct {
	Operator    string
	DNPath      string
	Value       string
	Placeholder string
}

type QueryParams struct {
	MainTable string
	Selects   []string
	Joins     []JoinPart
	Filters   []FilterPart
}

func TableNameToSQL1(tableName string) string {
	parts := strings.Split(tableName, ".")
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts[:len(parts)-1], "_") + "_" + parts[len(parts)-1]
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
	fromClause := fmt.Sprintf("FROM %s", qp.MainTable)

	// Building JOIN clause
	joinClauses := []string{}
	for _, join := range qp.Joins {
		joinSQL := fmt.Sprintf("%s JOIN %s ON %s = %s", join.JoinType, TableNameToSQL(strings.Split(join.Right, ".")[0]), ColumnNameToSQL(join.Left), ColumnNameToSQL(join.Right))
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

	// Assembling the final SQL query
	query := fmt.Sprintf("%s %s %s %s", selectClause, fromClause, strings.Join(joinClauses, " "), whereClause)
	return query, valuesMap, nil
}

func main() {
	//	rrawQuery := "dn=foo.bar&select=foo.bar.id&select=foo.bar.name&link=baz.qux&link=left:foo.bar.id:baz.qux.id&filter=match:foo.bar.name:John"

	rawQuery := "dn=foo.bar&select=foo.bar.id&select=foo.bar.name&link=baz.qux&link=left:foo.bar.id:baz.qux.id&filter=match:foo.bar.name:John:Wick"
	simple := "dn=domain.arp"

	queries := []string{
		rawQuery,
		simple,
		"dn=domain.arp&link=domain.arp.standard_id:standard.id",
		"dn=domain.arp&link=domain.arp.standard_id:standard.id&filter=match:domain.arp.ip_address:172.23.49.175",
		"dn=domain.arp&link=domain.arp.standard_id:standard.id&filter=match:domain.arp.ip_address:172.23.49.175&filter=match:domain.arp.device:eth1",
	}
	expected := []string{
		`SELECT foo_bar.id, foo_bar.name FROM foo_bar INNER JOIN baz ON foo_bar.id = baz.qux LEFT JOIN baz ON foo_bar.id = baz_qux.id WHERE foo_bar.name match 'John:Wick'`,
		`SELECT * FROM domain.arp`,
		`SELECT * FROM domain.arp INNER JOIN standard ON domain_arp.standard_id = standard.id`,
		`SELECT * FROM domain.arp INNER JOIN standard ON domain_arp.standard_id = standard.id WHERE domain.arp.ip_address = '172.23.49.175'`,
		`SELECT * FROM "domain.arp" AS domain_arp INNER JOIN standard ON domain_arp.standard_id = standard.id WHERE domain_arp.ip_address = $1 AND domain_arp.device = $2`,
	}
	for i, urlQuery := range queries {
		params, _ := url.ParseQuery(urlQuery)
		query, valuesMap, err := ConstructQuery(params)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println(i)
		fmt.Println(params)
		fmt.Println(urlQuery)
		fmt.Println(query)
		fmt.Println(valuesMap)
		if query != expected[i] {
			fmt.Println(expected[i])

			fmt.Println("Failed: ", i)
		}
	}
}
