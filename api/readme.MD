

list all the tables
```bash
curl 127.0.0.1:8080/api/list?format=json
```


static queries
  add them to 
```
var Queries = map[string]string{
    "arp":      `SELECT * FROM domain_arp`,
    "packages": `SELECT * FROM domain_packages`,
    "list":     `SELECT table_name FROM information_schema.tables WHERE table_schema='public'`,
    "columns":  `SELECT column_name, data_type FROM information_schema.columns`,
}
```

let's use the arp to get some data out.
```bash
curl "127.0.0.1:8080/api/arp?format=json" | python3 -m json.tool | less
```
```json
[
    {
        "device": "eth1",
        "device_id": "8ac3a60f-f483-52b1-9ec2-9f5bdd8501cd",
        "flags": "0x2",
        "hw_address": "00:50:56:b2:ca:da",
        "hw_type": "0x1",
        "ip_address": "172.23.49.175/32",
        "mask": "*"
    },
    {
        "device": "eth2",
        "device_id": "8ac3a60f-f483-52b1-9ec2-9f5bdd8501cd",
        "flags": "0x2",
        "hw_address": "d2:39:ea:41:98:47",
        "hw_type": "0x1",
        "ip_address": "192.168.101.172/32",
        "mask": "*"
    },
    {
        "device": "eth1",
        "device_id": "8ac3a60f-f483-52b1-9ec2-9f5bdd8501cd",
        "flags": "0x2",
        "hw_address": "00:50:56:b2:0c:8e",
        "hw_type": "0x1",
        "ip_address": "172.23.49.174/32",
        "mask": "*"
    },
    {
```




That is cool, but let's group the data per device.
```bash
curl "127.0.0.1:8080/api/arp?format=json&groupbykey=device_id" | python3 -m json.tool | less
```
```
{
    "00125471-f729-5809-9012-128769620456": [
        {
            "device": "eth0",
            "device_id": "00125471-f729-5809-9012-128769620456",
            "flags": "0x0",
            "hw_address": "00:00:00:00:00:00",
            "hw_type": "0x1",
            "ip_address": "169.254.169.254/32",
            "mask": "*"
        },
        {
            "device": "eth1",
            "device_id": "00125471-f729-5809-9012-128769620456",
            "flags": "0x2",
            "hw_address": "00:22:bd:f8:19:ff",
            "hw_type": "0x1",
            "ip_address": "172.23.60.1/32",
            "mask": "*"
        },
        {
            "device": "eth0",
            "device_id": "00125471-f729-5809-9012-128769620456",
            "flags": "0x2",
            "hw_address": "00:22:bd:f8:19:ff",
            "hw_type": "0x1",
            "ip_address": "172.23.100.1/32",
            "mask": "*"
        }
    ],
```



That is cool, but let's group it to device, and then interface.
```bash
curl "127.0.0.1:8080/api/arp?format=json&groupbykey=device_id&groupbykey2=device" | python3 -m json.tool  | less
```

```json
{
    "00125471-f729-5809-9012-128769620456": {
        "eth0": [
            {
                "device": "eth0",
                "device_id": "00125471-f729-5809-9012-128769620456",
                "flags": "0x0",
                "hw_address": "00:00:00:00:00:00",
                "hw_type": "0x1",
                "ip_address": "169.254.169.254/32",
                "mask": "*"
            },
            {
                "device": "eth0",
                "device_id": "00125471-f729-5809-9012-128769620456",
                "flags": "0x2",
                "hw_address": "00:22:bd:f8:19:ff",
                "hw_type": "0x1",
                "ip_address": "172.23.100.1/32",
                "mask": "*"
            }
        ],
        "eth1": [
            {
                "device": "eth1",
                "device_id": "00125471-f729-5809-9012-128769620456",
                "flags": "0x2",
                "hw_address": "00:22:bd:f8:19:ff",
                "hw_type": "0x1",
                "ip_address": "172.23.60.1/32",
                "mask": "*"
            }
        ]
    },
```
get all potential overlapping columns to join on


WITH domain_arp_columns AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'domain_arp' -- replace with the provided table name
)

, potential_joins AS (
    SELECT 
        table_name, 
        column_name,
        data_type
    FROM information_schema.columns 
    WHERE data_type IN (SELECT data_type FROM domain_arp_columns)
    AND table_name != 'domain_arp'
)

SELECT 
    pj.table_name AS joinable_table, 
    array_agg(pj.column_name) AS joinable_columns,
    mtc.column_name AS domain_arp_column
FROM potential_joins pj
JOIN domain_arp_columns mtc ON mtc.data_type = pj.data_type
GROUP BY pj.table_name, mtc.column_name
ORDER BY pj.table_name;



get all potential overlappting based on name

WITH main_table_columns AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'domain_arp' -- replace with the provided table name
)

, potential_joins AS (
    SELECT 
        table_name, 
        column_name
    FROM information_schema.columns 
    WHERE column_name IN (SELECT column_name FROM main_table_columns)
    AND table_name != 'domain_arp'
)

SELECT 
    pj.table_name AS joinable_table, 
    array_agg(pj.column_name) AS joinable_columns
FROM potential_joins pj
GROUP BY pj.table_name
ORDER BY pj.table_name;

}



get all overlapping that are not text
domain_arp is used as main table as example

WITH main_table_columns AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'domain_arp' -- replace with the provided table name
    AND data_type NOT IN ('text') -- Exclude text columns
)

SELECT 
    'domain_arp' AS main_table,
    mtc.column_name AS main_column,
    ic.table_name AS other_table,
    ic.column_name AS other_column,
    ic.data_type
FROM main_table_columns mtc
JOIN information_schema.columns ic ON mtc.data_type = ic.data_type
WHERE ic.table_name != 'domain_arp'
ORDER BY mtc.column_name, ic.table_name;



get all overlapping that are not text
domain_arp is used as main table as example

WITH main_table_columns AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'domain_arp' -- replace with the provided table name
    AND data_type NOT IN ('text') -- Exclude text columns
)

SELECT 
    'domain_arp' AS main_table,
    mtc.column_name AS main_column,
    ic.table_name AS other_table,
    ic.column_name AS other_column,
    ic.data_type
FROM main_table_columns mtc
JOIN information_schema.columns ic ON mtc.data_type = ic.data_type
WHERE ic.table_name != 'domain_arp'
ORDER BY mtc.column_name, ic.table_name;



get all overlapping get all non text first
domain_arp is used as main table as example

WITH main_table_columns AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'domain_arp' -- replace with the provided table name
)

SELECT 
    'domain_arp' AS main_table,
    mtc.column_name AS main_column,
    ic.table_name AS other_table,
    ic.column_name AS other_column,
    ic.data_type
FROM main_table_columns mtc
JOIN information_schema.columns ic ON mtc.data_type = ic.data_type
WHERE ic.table_name != 'domain_arp'
ORDER BY 
    CASE WHEN ic.data_type = 'text' THEN 1 ELSE 0 END,
    mtc.column_name, 
    ic.table_name;









// URL DESIGN

URL Design Specification for SQL Query Builder
Main Table (Domain Name - dn):
Represents the primary table for the SQL query.
URL parameter: dn
Example: dn=foo.bar translates to SQL table name foo_bar.
Select Columns (select):
Specifies which columns to select. If not provided, defaults to all columns (*).
URL parameter: select
Can be specified multiple times for multiple columns.
Example: select=foo.bar.id&select=foo.bar.name translates to SQL SELECT foo_bar.id, foo_bar.name.
Join/Link Tables (link):
Represents the joining/linking of tables.
URL parameter: link
Structure:
If only target table is given, default to INNER JOIN with main table on default column (assumed id here). Example: link=baz.qux results in INNER JOIN baz_qux ON foo_bar.id = baz_qux.id.
If source and target columns are provided without join type, defaults to INNER JOIN. Example: link=foo.bar.id:baz.qux.id results in INNER JOIN baz_qux ON foo_bar.id = baz_qux.id.
If join type, source, and target columns are all provided, it directly specifies the join type. Example: link=left:foo.bar.id:baz.qux.id results in LEFT JOIN baz_qux ON foo_bar.id = baz_qux.id.
Filter Conditions (filter):
Represents the conditions to filter the rows.
URL parameter: filter
Structure: <operator>:<column>:<value>
Example: filter=match:foo.bar.name:John translates to SQL WHERE foo_bar.name = 'John'.
Given the above specification, the URL query dn=foo.bar&select=foo.bar.id&select=foo.bar.name&link=baz.qux&link=left:foo.bar.id:baz.qux.id&filter=match:foo.bar.name:John will produce the SQL query SELECT foo_bar.id, foo_bar.name FROM foo_bar INNER JOIN baz_qux ON foo_bar.id = baz_qux.id LEFT JOIN baz_qux ON foo_bar.id = baz_qux.id WHERE foo_bar.name = 'John'.



### Adjusted URL Design Specification for SQL Query Builder

4. **Filter Conditions (`filter`)**:
   - Represents the conditions to filter the rows.
   - URL parameter: `filter`
   - Structure: `<operator>:<dn>:<value>`
     - Here, `<operator>` is the filtering operator (e.g., `match`, `gt`, `lt`).
     - `<dn>` represents the fully qualified domain name for the column.
     - `<value>` is the value for the filter. It can contain any character including `:`. After splitting, everything after the second colon (position 1) is considered part of the value.
   - Example: `filter=match:foo.bar.name:John:Smith` translates to SQL `WHERE foo_bar.name = 'John:Smith'`.


