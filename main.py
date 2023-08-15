
import json

import os
import json
import re

uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

def is_uuid(item):
    return re.match(uuid_pattern, item)


def get_data(data_dir, uuid):
    return json.load(open(data_dir + uuid))


def get_postgres_type(field_type):
    """Map custom type names to PostgreSQL data types"""
    mapping = {
        "integer": "INTEGER",
        "uuid": "UUID PRIMARY KEY",
        "port": "INTEGER",
        "ipv4": "CIDR",
        "inserted": "TIMESTAMP",
        "updated": "TIMESTAMP"
    }
    return mapping.get(field_type, "TEXT")

def generate_create_table_sql(table_name, columns_data):
    sql = f"CREATE TABLE {table_name} (\n"
    for column, column_type in columns_data.items():
        sql += f"    \"{column}\" {column_type},\n"

    if table_name != "standard":
        sql += f"    FOREIGN KEY (device_id) REFERENCES standard(id),\n"

    sql = sql.rstrip(",\n")  # remove trailing comma and newline
    sql += "\n);"
    return sql


def generate_insert_template(table_name, columns):
    quoted_columns = [f'"{column}"' for column in columns]
    placeholders = ", ".join(["{%s}" % column for column in columns])
    return  f"INSERT INTO {table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders});"


# Load JSON data

def datas():
    single = False
    if single:
        data = json.load(open("/Volumes/shield/data/standalone/node/c0364f97-6104-5c89-85f6-fe3b88dee715"))
        yield data
    else:
        data_dir = "/Volumes/shield/data/standalone/node/"

        uuid_files = (file for file in os.listdir(data_dir) if is_uuid(file))
        for uuid_file in uuid_files:
            data = get_data(data_dir, uuid_file)
            yield data

# Consolidate table structures
tables = {}
for data in datas():
    for table_data in data:
        table_name = "_".join(table_data["_dn"])
        table_name = table_name.replace(" ", "_") # Should be removed

        columns = table_data["__columns"]
        if table_name not in tables:
            tables[table_name] = {"device_id": "UUID"}

        for column in columns:
            column_type = table_data.get(f"_{column}_field_type") or table_data.get(f"_{column}_type", "string")
            postgres_type = get_postgres_type(column_type)
            tables[table_name][column.replace("-", "_")] = postgres_type

# Generate CREATE TABLE statements
with open('init.sql', 'w') as f:
    for table_name, columns_data in tables.items():
        f.write(generate_create_table_sql(table_name, columns_data))
        f.write("\n\n---\n\n")

# Generate INSERT templates
insert_templates = {}
for table_name in tables.keys():
    columns = list(tables[table_name].keys())
    insert_templates[table_name] = generate_insert_template(table_name, columns)


# Use INSERT templates to create actual INSERT statements


def parse_value(item, column_type):
    postgres_type = get_postgres_type(column_type)
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

def create_formatted_data(record):
    formatted_data = defaultdict(default_value)
    for column in record['__columns']:
        key = column.replace("-", "_")
        value = parse_value(record[column], record.get(f"_{column}_field_type") or record.get(f"_{column}_type", "string"))
        formatted_data[key] = value
        formatted_data['device_id'] = f"'{record['common_id']}'"


    return formatted_data


class SafeDict(dict):
    def __missing__(self, key):
        return 'NULL'

from collections import defaultdict
def default_value():
    return 'NULL'

with open('inserts.sql', 'w') as f:
    for data in datas():
        for record in data:
            table_name = "_".join(record["_dn"]).replace(" ", "_")
            #formatted_data = {col: record.get(col, 'NULL') for col in record['__columns']}
            formatted_data = create_formatted_data(record)
            insert_statement = insert_templates[table_name].format_map(formatted_data)

            f.write(insert_statement)
            f.write("\n")

