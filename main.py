import os
import sys
import re
import csv
import json
import time
import logging

import gzip

from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

META_RBAC_KEY = "__meta__rbac_read_groups"
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


def generate_create_table_sql(table_name, columns_data):
    sql_parts = [f"CREATE TABLE \"{table_name}\" ("]

    for column, column_type in columns_data.items():
        if table_name == "standard" and column == "id":
            sql_parts.append(f"    \"{column}\" {column_type} PRIMARY KEY,")
        else:
            sql_parts.append(f"    \"{column}\" {column_type},")

    if table_name != "standard":
        sql_parts.append("    FOREIGN KEY (standard_id) REFERENCES standard(id),")

    sql_parts[-1] = sql_parts[-1].rstrip(",")  # remove trailing comma from the last column

    sql_parts.append(");")
    return "\n".join(sql_parts)


def generate_insert_template(table_name, columns):
    quoted_columns = [f'"{column}"' for column in columns]
    placeholders = ", ".join(["{%s}" % column for column in columns])
    return f"INSERT INTO \"{table_name}\" ({', '.join(quoted_columns)}) VALUES ({placeholders});"


def old_way_of_getting_data():
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


def read_gzip(file_path):
    with gzip.open(file_path, 'rt') as gz_file:
        for line in gz_file:
            yield json.loads(line.strip())


def datas(data_base_dir):
    single = False
    file_path = f"{data_base_dir}standalone/node/large_file.gzip"
    if single:
        with gzip.open(file_path, 'rt') as gz_file:
            for line in gz_file:
                yield json.loads(line.strip())
                break
    else:
        yield from read_gzip(file_path)


def create_initial_data(data_base_dir):
    tables = {}
    for data in datas(data_base_dir):
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

            tables[table_name][META_RBAC_KEY] = "text[]"
    return tables


def create_schema(tables):
    # Generate CREATE TABLE statements
    with open('init.sql', 'w') as f:
        for table_name, columns_data in tables.items():
            f.write(generate_create_table_sql(table_name, columns_data))
            f.write("\n\n---\n\n")


def parse_value_csv(item, dsm_type, field_type):
    #  If custom some extra processing is needed based on dsm type.
    #  based on dsm that could be done here.
    # postgres_type = get_postgres_type(dsm_type, field_type)
    if isinstance(item, list):
        # Convert the list to PostgreSQL array format
        return '{' + ', '.join([f'{x}' for x in item]) + '}'
    return item


def get_row_level_read_access(record):
    columns = record.get("__columns")
    if columns is None:
        raise Exception("No column record", json.dumps(record, indent=4))
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
    formatted_data[META_RBAC_KEY] = parse_value_csv(get_row_level_read_access(record), None, None)
    for column in record['__columns']:
        value = parse_value_csv(record[column], record.get(f"_{column}_type", "string"), record.get(f"_{column}_field_type"))
        formatted_data[column] = value

    return formatted_data


def default_value():
    return 'NULL'


class CSVWriter():
    def __init__(self, unique_columns_per_table, base_dir_data="files"):
        self.unique_columns_per_table = unique_columns_per_table
        self.file_handlers = {}
        self.csv_writers = {}
        self.base_dir_data = base_dir_data

    def write(self, table_name, formatted_data):
        if table_name not in self.file_handlers:
            self.setup(table_name)

        csv_writer = self.csv_writers[table_name]
        csv_writer.writerow(formatted_data)

    def setup(self, table_name):
        file_handler = open(f"{self.base_dir_data}/{table_name}.csv", 'w', newline='')
        csv_writer = csv.DictWriter(file_handler, fieldnames=self.unique_columns_per_table[table_name])
        csv_writer.writeheader()

        self.file_handlers[table_name] = file_handler
        self.csv_writers[table_name] = csv_writer

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for _, file_handler in self.file_handlers.items():
            file_handler.close()


def create_data(data_base_dir, write_base_dir, initial_data):
    unique_columns = {table_name: list(columns.keys()) for table_name, columns in initial_data.items()}
    with CSVWriter(unique_columns, write_base_dir) as csv_writer:
        for data in datas(data_base_dir):
            for record in data:
                table_name = ".".join(record["_dn"])
                formatted_data = create_formatted_data(record)
                csv_writer.write(table_name, formatted_data)


def get_postgres_type(dsm_type, field_type):
    """
    Map custom type names to PostgreSQL data types.

    Args:
        dsm_type (str): Refers to the base type. Common base types include 'str', 'int', and 'float'.
        field_type (str): Refers to the higher data type, representing the meaning of the data.
                          For instance, an IP might be stored as a string, but its higher type would indicate its IP nature.

    Returns:
        str: The corresponding PostgreSQL data type.

    Note:
        This function utilizes the global variable `DSM_TO_PSQL_MAPPING` to get the mappings. This is to avoid
        the overhead of querying the same data repeatedly (avoiding query n+1). The data in `DSM_TO_PSQL_MAPPING`
        is sourced from the 'dsm_psql_types.csv' file.
    """
    base_type = DSM_TO_PSQL_MAPPING.get(dsm_type, "TEXT")
    higher_type = DSM_TO_PSQL_MAPPING.get(field_type)

    return higher_type or base_type


if __name__ == "__main__":
    run_create_schema = True
    run_create_data = True
    default_data_base_dir = "/Volumes/shield/data/"
    default_write_base_dir = "/Volumes/shield/data/__meta__/nodes/files"

    data_base_dir = sys.argv[sys.argv.index("-data")+1] if "-data" in sys.argv else default_data_base_dir
    write_base_dir = sys.argv[sys.argv.index("-write")+1] if "-write" in sys.argv else default_write_base_dir

    DSM_TO_PSQL_MAPPING, _ = get_dsm_psql_types()
    initial_data = create_initial_data(data_base_dir)

    time_start_total = time.time()
    if run_create_schema:
        time_start = time.time()
        logging.info("Starting create schema")
        create_schema(initial_data)
        logging.info("Done with create schema. Time taken: %s seconds", round(time.time() - time_start, 2))
    if run_create_data:
        time_start = time.time()
        logging.info("Starting creation of data files")
        create_data(data_base_dir, write_base_dir, initial_data)
        logging.info("Done with data files. Time taken: %s seconds", round(time.time() - time_start, 2))

    logging.info("Postgres setup complete. Time taken: %s seconds", round(time.time() - time_start_total, 2))
