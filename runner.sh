#!/bin/bash

# Define base path
BASE_PATH="/Volumes/shield/data/__meta__/nodes"

# Create the sql files, table schema and data
pypy3 /Volumes/shield/Development/dsm_nodes/main.py -write $BASE_PATH/files

# Combine SQL files
echo "-- Combining SQL and CSV commands"
echo "\set AUTOCOMMIT off" > $BASE_PATH/combined.sql
cat $BASE_PATH/init.sql >> $BASE_PATH/combined.sql
rm $BASE_PATH/init.sql

# First, import standard.csv if it exists
if [[ -f $BASE_PATH/files/standard.csv ]]; then
    echo "-- Starting PostgreSQL"
    echo "COPY \"standard\" FROM '$BASE_PATH/files/standard.csv' DELIMITER ',' CSV HEADER;" >> $BASE_PATH/combined.sql
fi

# Then process other CSV files
for csv in $BASE_PATH/files/*.csv; do
    # Skip standard.csv since we've already processed it
    if [[ "$csv" != "$BASE_PATH/files/standard.csv" ]]; then
        table_name=$(basename "$csv" .csv)
        echo "COPY \"$table_name\" FROM '$BASE_PATH/$csv' DELIMITER ',' CSV HEADER;" >> $BASE_PATH/combined.sql
    fi
done

echo "COMMIT;" >> $BASE_PATH/combined.sql

# Add VACUUM FULL ANALYZE after COMMIT
echo "VACUUM FULL ANALYZE;" >> $BASE_PATH/combined.sql

# Compress the combined SQL file
echo "-- Compressing combined SQL"
rm $BASE_PATH/combined.sql.gz 2>/dev/null
gzip $BASE_PATH/combined.sql

# Check if the container exists, and if it does, stop and remove it
if docker ps -a | grep -q 'dsm-nodes-storage-container'; then
    echo "-- Restarting PostgreSQL container"
    docker stop my-postgres-container && docker rm my-postgres-container
fi

# Start PostgreSQL with combined.sql.gz and bind mount the files directory
echo "-- Starting PostgreSQL"
docker run --name my-postgres-container \
-e POSTGRES_PASSWORD=mysecretpassword \
-e POSTGRES_DB=mydatabase \
-v $BASE_PATH/combined.sql.gz:/docker-entrypoint-initdb.d/combined.sql.gz \
-v $BASE_PATH/files:/docker-entrypoint-initdb.d/files \
--health-cmd 'pg_isready -U postgres' \
--health-interval 10s \
--health-timeout 5s \
--health-retries 5 \
-p 5432:5432 \
-d postgres:latest
