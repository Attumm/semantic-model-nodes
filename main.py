import os
import re
import csv
import json

from collections import defaultdict

uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

def is_uuid(item):
    return re.match(uuid_pattern, item)


def get_data(data_dir, uuid):
    return json.load(open(data_dir + uuid))


def get_dsm_psql_types(path_to_csv='dsm_psql_types.csv'):
    dsm_to_psql, psql_to_dsm = {}, {}
    with open(path_to_csv, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dsm_to_psql[row['dsm_type']] = row['psql_type']
            psql_to_dsm[row['psql_type']] = row['dsm_type']
    return dsm_to_psql, psql_to_dsm


DSM_TO_PSQL_MAPPING, _ = get_dsm_psql_types()

def get_postgres_type(dsm_type, field_type):
    """Map custom type names to PostgreSQL data types"""
    base_type = DSM_TO_PSQL_MAPPING.get(dsm_type, "TEXT")
    higher_type = DSM_TO_PSQL_MAPPING.get(field_type)
    
    return higher_type or base_type


def generate_create_table_sql(table_name, columns_data):
    sql_parts = [f"CREATE TABLE \"{table_name}\" ("]

    for column, column_type in columns_data.items():
        if table_name == "standard" and column == "id":
            sql_parts.append(f"    \"{column}\" {column_type} PRIMARY KEY,")
        else:
            sql_parts.append(f"    \"{column}\" {column_type},")

    if table_name != "standard":
        sql_parts.append(f"    FOREIGN KEY (standard_id) REFERENCES standard(id),")

    sql_parts[-1] = sql_parts[-1].rstrip(",")  # remove trailing comma from the last column

    sql_parts.append(");")
    return "\n".join(sql_parts)


def generate_insert_template(table_name, columns):
    quoted_columns = [f'"{column}"' for column in columns]
    placeholders = ", ".join(["{%s}" % column for column in columns])
    return  f"INSERT INTO \"{table_name}\" ({', '.join(quoted_columns)}) VALUES ({placeholders});"


def datas():
    single = False
    if single:
        data = json.load(open("/Volumes/shield/data/standalone/node/f658d592-7845-5054-899d-b98249322b30"))

        yield data
    else:
        data_dir = "/Volumes/shield/data/standalone/node/"

        uuid_files = (file for file in os.listdir(data_dir) if is_uuid(file))
        for uuid_file in uuid_files:
            data = get_data(data_dir, uuid_file)
            yield data

# Consolidate table structures
tables = {}

tables = {}
for data in datas():
    for table_data in data:
        table_name = ".".join(table_data["_dn"])

        columns = table_data["__columns"]
        if table_name not in tables:
            if table_name != "standard":
                tables[table_name] = {"standard_id": "UUID"}
            else:
                tables[table_name] = {}

        for column in columns:
            postgres_type = get_postgres_type(table_data.get(f"_{column}_type", "string"), table_data.get(f"_{column}_field_type"))
            tables[table_name][column] = postgres_type

        # Add the common read_groups column for each table
        tables[table_name]["read_groups"] = "text[]"

# Generate CREATE TABLE statements
with open('init.sql', 'w') as f:
    for table_name, columns_data in tables.items():
        f.write(generate_create_table_sql(table_name, columns_data))
        f.write("\n\n---\n\n")

print("done with init.sql")

# Generate INSERT templates
insert_templates = {}
for table_name in tables.keys():
    columns = list(tables[table_name].keys())
    insert_templates[table_name] = generate_insert_template(table_name, columns)


def parse_value_insert(item, dsm_type, field_type):
    postgres_type = get_postgres_type(dsm_type, field_type)
    if item == 'None':
        return 'NULL'
    elif postgres_type == "CIDR" and not item:
        return "NULL"
    elif type(item) == str:
        return f"'{item.strip()}'"
    elif type(item) == list:  # Check if it's an array type and item is a list
        # Convert the list to a PostgreSQL array literal
        if len(item) == 0:
            return 'NULL'
        return "{" + ",".join([f"'{x}'" for x in item]) + "}"
    elif item is None:
        return 'NULL'
    
    return item


def parse_value_csv(item, dsm_type, field_type):
    postgres_type = get_postgres_type(dsm_type, field_type)
    #if item == 'None':
    #    return 'NULL'
    #elif postgres_type == "CIDR" and not item:
    #    return "NULL"
    #elif item is None:
    #    return NULL 


    #if isinstance(item, str) and item.startswith("[") and item.endswith("]"):
        # Convert Python list representation to PostgreSQL array representation
   #     item = "{" + item[1:-1] + "}"
   #     item = item.replace("'", "\"")  # Use double quotes for strings within array

    # TODO tired
    #if isinstance(item, str) and len(item) > 0 and item[0] == "'" and item[-1] == "'":
    #    return item[1:-1]
    if isinstance(item, list):
        # Convert the list to PostgreSQL array format
        return '{' + ', '.join([f'"{x}"' for x in item]) + '}'
    return item


def get_row_level_read_access(record):
    columns = record.get("__columns")
    if columns is None:
        return []
    if not isinstance(columns, list) or len(columns) <= 0:
        return []

    row_level_rbac_read = set(record[f"_{columns[0]}_rbac_read"])

    for col in columns[1:]:
        rbac_key = f"_{col}_rbac_read"
        row_level_rbac_read &= set(record[rbac_key])

    return list(row_level_rbac_read)


def create_formatted_data(record):
    formatted_data = defaultdict(default_value)
    if record["_dn"] != ["standard"]:
        formatted_data['standard_id'] = f"{record['common_id']}"
    formatted_data['read_groups'] = parse_value_csv(get_row_level_read_access(record), None, None)
    for column in record['__columns']:
        #value = parse_value_insert(record[column], record.get(f"_{column}_type", "string"), record.get(f"_{column}_field_type")) 
        value = parse_value_csv(record[column], record.get(f"_{column}_type", "string"), record.get(f"_{column}_field_type")) 
        formatted_data[column] = value

    return formatted_data


class SafeDict(dict):
    def __missing__(self, key):
        return 'NULL'

from collections import defaultdict
def default_value():
    return 'NULL'

"""
with open('inserts.sql', 'w') as f:
    for data in datas():
        for record in data:
            table_name = ".".join(record["_dn"])#.replace(" ", "_")
            #formatted_data = {col: record.get(col, 'NULL') for col in record['__columns']}
            formatted_data = create_formatted_data(record)
            insert_statement = insert_templates[table_name].format_map(formatted_data)

            f.write(insert_statement)
            f.write("\n")

"""

class CSVWriter():
    def __init__(self, unique_columns_per_table):
        self.unique_columns_per_table = unique_columns_per_table
        self.file_handlers = {}
        self.csv_writers = {}

    def write(self, table_name, formatted_data):
        if table_name not in self.file_handlers:
            self.setup(table_name)

        csv_writer = self.csv_writers[table_name]
        csv_writer.writerow(formatted_data)

    def setup(self, table_name):
        file_handler = open(f"files/{table_name}.csv", 'w', newline='')
        csv_writer = csv.DictWriter(file_handler, fieldnames=self.unique_columns_per_table[table_name])
        csv_writer.writeheader()

        self.file_handlers[table_name] = file_handler
        self.csv_writers[table_name] = csv_writer

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for _, file_handler in self.file_handlers.items():
            file_handler.close()


unique_columns = {table_name: list(columns.keys()) for table_name, columns in tables.items()}
with CSVWriter(unique_columns) as csv_writer:
    for data in datas():
        for record in data:
            table_name = ".".join(record["_dn"])
            #print(record)
            #print("----")
            formatted_data = create_formatted_data(record)
            csv_writer.write(table_name, formatted_data)


