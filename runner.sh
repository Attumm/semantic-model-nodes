#!/bin/bash
set -e

DB_NAME="mydatabase"
DB_PASSWORD="mysecretpassword"
DOCKER_CONTAINER_NAME="sm-nodes-storage"


POSTGRES_IMAGE="postgres:latest"
POSTGRES_USER="postgres"
POSTGRES_PORT="5432"
HOST_PORT="5432"

# Define base path
BASE_PATH="/Volumes/shield/data/__meta__/node"

# Make sure directory structure exists
mkdir -p $BASE_PATH/files

# Create the sql files, table schema and data
pypy3 /Volumes/shield/Development/dsm_nodes/main.py -write $BASE_PATH

# Combine SQL files
echo "-- Combining SQL and CSV commands"
echo "\set AUTOCOMMIT off" > $BASE_PATH/combined.sql
cat $BASE_PATH/init.sql >> $BASE_PATH/combined.sql


# First, import standard.csv if it exists
if [[ -f $BASE_PATH/files/standard.csv ]]; then
    echo "-- Starting PostgreSQL"
    echo "COPY \"standard\" FROM '/docker-entrypoint-initdb.d/files/standard.csv' DELIMITER ',' CSV HEADER;" >> $BASE_PATH/combined.sql
fi

# Then process other CSV files
for csv in $BASE_PATH/files/*.csv; do
    # Skip standard.csv since we've already processed it
    if [[ "$csv" != "$BASE_PATH/files/standard.csv" ]]; then
        table_name=$(basename "$csv" .csv)
        echo "COPY \"$table_name\" FROM '/docker-entrypoint-initdb.d/files/$(basename "$csv")' DELIMITER ',' CSV HEADER;" >> $BASE_PATH/combined.sql
    fi
done

echo "COMMIT;" >> $BASE_PATH/combined.sql

# Add VACUUM FULL ANALYZE after COMMIT
echo "VACUUM FULL ANALYZE;" >> $BASE_PATH/combined.sql

# Compress the combined SQL file
echo "-- Compressing combined SQL"
echo "Remove old"
rm -f $BASE_PATH/combined.sql.gz || true
echo "Create new"
gzip $BASE_PATH/combined.sql

# Check if the container exists, and if it does, stop and remove it
if docker ps -a | grep -q "$DOCKER_CONTAINER_NAME"; then
    echo "-- Restarting PostgreSQL container"
    docker stop $DOCKER_CONTAINER_NAME && docker rm $DOCKER_CONTAINER_NAME
fi

# Start PostgreSQL with combined.sql.gz and bind mount the files directory
echo "-- Starting PostgreSQL"
docker run --name $DOCKER_CONTAINER_NAME \
-e POSTGRES_USER=$POSTGRES_USER \
-e POSTGRES_PASSWORD=$DB_PASSWORD \
-e POSTGRES_DB=$DB_NAME \
-v $BASE_PATH/combined.sql.gz:/docker-entrypoint-initdb.d/combined.sql.gz \
-v $BASE_PATH/files:/docker-entrypoint-initdb.d/files \
--health-cmd 'pg_isready -U postgres' \
--health-interval 10s \
--health-timeout 5s \
--health-retries 5 \
-p $HOST_PORT:$POSTGRES_PORT \
-d $POSTGRES_IMAGE
