package main

import (
	"fmt"
	"net/url"
	"strings"
)

// 1. Operator Map
var SQLOperators1 = map[string]string{
	"MATCH":                "= %s",
	"NOTMATCH":             "!= %s",
	"IMATCH":               "ILIKE %s",
	"STARTSWITH":           "LIKE %s || '%%'",
	"ISTARTSWITH":          "ILIKE %s || '%%'",
	"ENDSWITH":             "LIKE '%%' || %s",
	"IENDSWITH":            "ILIKE '%%' || %s",
	"TEXT_CONTAINS":        "LIKE '%%' || %s || '%%'",
	"ITEXT_CONTAINS":       "ILIKE '%%' || %s || '%%'",
	"GT":                   "> %s",
	"LT":                   "< %s",
	"LTE":                  "<= %s",
	"GTE":                  ">= %s",
	"IN":                   "IN (%s)",
	"NOTIN":                "NOT IN (%s)",
	"NEQ":                  "<> %s",
	"CONTAINED_BY_OR_EQ":   ">>= %s",
	"CONTAINS_OR_EQ":       "<<= %s",
	"CONTAINED_BY":         ">> %s",
	"CONTAINS":             "<< %s",
	"IS_SUPERNET_OR_EQ":    "~>= %s",
	"IS_SUBNET_OR_EQ":      "~<= %s",
	"IS_SUPERNET":          "~> %s",
	"IS_SUBNET":            "~< %s",
	"BEFORE":               "< %s",
	"AFTER":                "> %s",
	"ON_OR_BEFORE":         "<= %s",
	"ON_OR_AFTER":          ">= %s",
	"BETWEEN":              "BETWEEN %s AND %s",
	"NOT_BETWEEN":          "NOT BETWEEN %s AND %s",
	"ARRAY_CONTAINS":       " @> ARRAY[%s]", // Array contains
	"ARRAY_IS_CONTAINED":   "<@ ARRAY[%s]",  // Array is contained by
	"ARRAY_OVERLAPS":       "&& ARRAY[%s]",  // Array overlaps
	"ARRAY_MATCH":          "= ARRAY[%s]",   // Array equality
	"ARRAY_NOTMATCH":       "!= ARRAY[%s]",  // Array inequality
	"ARRAY_ELEMENT_MATCH":  "ANY(%s)",       // Array element equality (checks if array has the element)
	"ARRAY_REMOVE_ELEMENT": "- %s",          // Array remove a specified element (for int array example)
	"ARRAY_HAS_ELEMENT":    "? %s",          // Array contains a specified element (checks if array has the element, alternative way)
	"ARRAY_GT":             "> ARRAY[%s]",   // Greater than, compares arrays lexicographically
	"ARRAY_LT":             "< ARRAY[%s]",   // Less than
	"ARRAY_GTE":            ">= ARRAY[%s]",  // Greater than or equal
	"ARRAY_LTE":            "<= ARRAY[%s]",  // Less than or equal
}

// 2. Allowed Operator Map
var AllowedOperators1 = map[string][]string{
	"text":     {"MATCH", "NOTMATCH", "IMATCH", "STARTSWITH", "ISTARTSWITH", "ENDSWITH", "IENDSWITH", "CONTAINS", "ICONTAINS"},
	"cidr":     {"MATCH", "NEQ", "CONTAINED_BY_OR_EQ", "CONTAINS_OR_EQ", "CONTAINED_BY", "CONTAINS", "IS_SUPERNET_OR_EQ", "IS_SUBNET_OR_EQ", "IS_SUPERNET", "IS_SUBNET"},
	"int":      {"MATCH", "GT", "LT", "LTE", "GTE", "IN", "NOTIN", "NEQ"},
	"uuid":     {"MATCH", "NOTMATCH"},
	"timezone": {"MATCH", "NOTMATCH", "BEFORE", "AFTER", "ON_OR_BEFORE", "ON_OR_AFTER", "BETWEEN", "NOT_BETWEEN", "IN", "NOTIN"},
	"array": {"ARRAY_CONTAINS", "ARRAY_IS_CONTAINED", "ARRAY_OVERLAPS", "ARRAY_MATCH", "ARRAY_NOTMATCH", "ARRAY_ELEMENT_MATCH", "ARRAY_CONCAT", "ARRAY_REMOVE_ELEMENT",
		"ARRAY_HAS_ELEMENT", "ARRAY_GT", "ARRAY_LT", "ARRAY_GTE", "ARRAY_LTE"},
}

var SQLOperators = map[string]string{
	"match":              "= %s",
	"notmatch":           "!= %s",
	"imatch":             "ILIKE %s",
	"startswith":         "LIKE %s || '%%'",
	"istartswith":        "ILIKE %s || '%%'",
	"endswith":           "LIKE '%%' || %s",
	"iendswith":          "ILIKE '%%' || %s",
	"contains":           "LIKE '%%' || %s || '%%'",
	"icontains":          "ILIKE '%%' || %s || '%%'",
	"gt":                 "> %s",
	"lt":                 "< %s",
	"lte":                "<= %s",
	"gte":                ">= %s",
	"in":                 "IN (%s)",
	"notin":              "NOT IN (%s)",
	"neq":                "<> %s",
	"contained_by_or_eq": ">>= %s",
	"contains_or_eq":     "<<= %s",
	"contained_by":       ">> %s",
	//"contains":           "<< %s",
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

var AllowedOperators = map[string][]string{
	"text":     {"match", "notmatch", "imatch", "startswith", "istartswith", "endswith", "iendswith", "contains", "icontains"},
	"cidr":     {"match", "neq", "contained_by_or_eq", "contains_or_eq", "contained_by", "contains", "is_supernet_or_eq", "is_subnet_or_eq", "is_supernet", "is_subnet"},
	"int":      {"match", "gt", "lt", "lte", "gte", "in", "notin", "neq"},
	"uuid":     {"match", "notmatch"},
	"timezone": {"match", "notmatch", "before", "after", "on_or_before", "on_or_after", "between", "not_between", "in", "notin"},
	"array": {"array_contains", "array_is_contained", "array_overlaps", "array_match", "array_notmatch", "array_element_match", "array_concat", "array_remove_element",
		"array_has_element", "array_gt", "array_lt", "array_gte", "array_lte"},
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
	return SQLOperators[strings.ToLower(operator)]
}

func ParseQueryParams(params url.Values) (*QueryParams, error) {
	qp := &QueryParams{}
	// Parse main table (dn)
	qp.MainTable = TableNameToSQL(params.Get("dn"))
	// Parse select fields
	qp.Selects = params["field"]

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
	//	rrawQuery := "dn=foo.bar&field=foo.bar.id&field=foo.bar.name&link=baz.qux&link=left:foo.bar.id:baz.qux.id&filter=match:foo.bar.name:John"

	rawQuery := "dn=foo.bar&field=foo.bar.id&field=foo.bar.name&link=baz.qux&link=left:foo.bar.id:baz.qux.id&filter=match:foo.bar.name:John:Wick"
	simple := "dn=domain.arp"

	queries := []string{
		rawQuery,
		simple,
		// "dn=domain.arp&field=
		"dn=domain.arp&link=domain.arp.standard_id:standard.id",
		"dn=domain.arp&link=domain.arp.standard_id:standard.id&filter=match:domain.arp.ip_address:172.23.49.175",
		"dn=domain.arp&link=domain.arp.standard_id:standard.id&filter=match:domain.arp.ip_address:172.23.49.175&filter=match:domain.arp.device:eth1",
		"dn=domain.address&field=domain.address.standard_id&field=domain.address.value&field=domain.address.read_groups&orderby=asc:domain.address.value",
		"dn=domain.cpu_model_info&field=domain.cpu_model_info.standard_id&field=domain.cpu_model_info.architecture&field=domain.cpu_model_info.cpu_op_modes&field=domain.cpu_model_info.byte_order&field=domain.cpu_model_info.cpus&field=domain.cpu_model_info.online_cpus_list&field=domain.cpu_model_info.threads_per_core&field=domain.cpu_model_info.cores_per_socket&field=domain.cpu_model_info.sockets&field=domain.cpu_model_info.vendor_id&field=domain.cpu_model_info.cpu_family&field=domain.cpu_model_info.model&field=domain.cpu_model_info.model_name&field=domain.cpu_model_info.stepping&field=domain.cpu_model_info.cpu_mhz&field=domain.cpu_model_info.bogomips&field=domain.cpu_model_info.hypervisor_vendor&field=domain.cpu_model_info.virtualization_type&field=domain.cpu_model_info.l1d_cache&field=domain.cpu_model_info.l1i_cache&field=domain.cpu_model_info.l2_cache&field=domain.cpu_model_info.flags&field=domain.cpu_model_info.read_groups",
		"dn=domain.arp&field=domain.arp.ip_address&field=domain.arp.hw_type&field=domain.arp.flags&field=domain.arp.hw_address&field=domain.arp.mask&field=domain.arp.device",
		"dn=domain.cpu_model_info&field=domain.cpu_model_info.cpu_family&field=domain.cpu_model_info.model&field=domain.cpu_model_info.model_name&field=domain.cpu_model_info.cpu_mhz",
		"dn=domain.cpu_model_info&field=domain.cpu_model_info.model_name&field=domain.cpu_model_info.cpus&field=domain.cpu_model_info.model&field=domain.cpu_model_info.threads_per_core&field=domain.cpu_model_info.cpu_family&field=domain.cpu_model_info.cpu_mhz",
		"dn=domain.cpu_model_info&field=domain.cpu_model_info.model_name&field=domain.cpu_model_info.cpu_mhz&filter=istartswith:domain.cpu_model_info.model_name:intel xeon",
		"dn=domain.cpu_model_info&field=domain.cpu_model_info.model_name&field=domain.cpu_model_info.cpu_mhz&filter=icontains:domain.cpu_model_info.model_name:skylake&filter=istartswith:domain.cpu_model_info.model_name:intel xeon",
	}
	expected := []string{

		`SELECT foo_bar.id, foo_bar.name FROM foo_bar INNER JOIN baz ON foo_bar.id = baz.qux LEFT JOIN baz ON foo_bar.id = baz_qux.id WHERE foo_bar.name match 'John:Wick'`,
		`SELECT * FROM domain.arp`,
		`SELECT * FROM domain.arp INNER JOIN standard ON domain_arp.standard_id = standard.id`,
		`SELECT * FROM domain.arp INNER JOIN standard ON domain_arp.standard_id = standard.id WHERE domain.arp.ip_address = '172.23.49.175'`,
		`SELECT * FROM "domain.arp" AS domain_arp INNER JOIN standard ON domain_arp.standard_id = standard.id WHERE domain_arp.ip_address = $1 AND domain_arp.device = $2`,
		`SELECT domain_address.standard_id, domain_address.value, domain_address.read_groups FROM "domain.address" AS domain_address`,
		`SELECT domain_cpu_model_info.standard_id, domain_cpu_model_info.architecture, domain_cpu_model_info.cpu_op_modes, domain_cpu_model_info.byte_order, domain_cpu_model_info.cpus, domain_cpu_model_info.online_cpus_list, domain_cpu_model_info.threads_per_core, domain_cpu_model_info.cores_per_socket, domain_cpu_model_info.sockets, domain_cpu_model_info.vendor_id, domain_cpu_model_info.cpu_family, domain_cpu_model_info.model, domain_cpu_model_info.model_name, domain_cpu_model_info.stepping, domain_cpu_model_info.cpu_mhz, domain_cpu_model_info.bogomips, domain_cpu_model_info.hypervisor_vendor, domain_cpu_model_info.virtualization_type, domain_cpu_model_info.l1d_cache, domain_cpu_model_info.l1i_cache, domain_cpu_model_info.l2_cache, domain_cpu_model_info.flags, domain_cpu_model_info.read_groups FROM "domain.cpu_model_info" AS domain_cpu_model_info;`,
		`SELECT domain_arp.ip_address, domain_arp.hw_type, domain_arp.flags, domain_arp.hw_address, domain_arp.mask, domain_arp.device FROM "domain.arp" AS domain_arp`,
		``,
		``,
		`SELECT domain_cpu_model_info.model_name, domain_cpu_model_info.cpu_mhz FROM "domain.cpu_model_info" AS domain_cpu_model_info  WHERE domain_cpu_model_info.model_name ILIKE $1 || '%'`,
		``,
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
