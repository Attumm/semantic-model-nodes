#!/bin/bash
time pypy3 main.py

# Combine SQL files
echo "-- Combining SQL and CSV commands"
echo "\set AUTOCOMMIT off" > combined.sql
cat init.sql >> combined.sql

# First, import standard.csv if it exists
if [[ -f files/standard.csv ]]; then
    echo "COPY \"standard\" FROM '/docker-entrypoint-initdb.d/files/standard.csv' DELIMITER ',' CSV HEADER;" >> combined.sql
fi

# Then process other CSV files
for csv in files/*.csv; do
    # Skip standard.csv since we've already processed it
    if [[ "$csv" != "files/standard.csv" ]]; then
        table_name=$(basename "$csv" .csv)
        echo "COPY \"$table_name\" FROM '/docker-entrypoint-initdb.d/$csv' DELIMITER ',' CSV HEADER;" >> combined.sql
    fi
done

echo "COMMIT;" >> combined.sql

# Add VACUUM FULL ANALYZE after COMMIT
echo "VACUUM FULL ANALYZE;" >> combined.sql

# Compress the combined SQL file
echo "-- Compressing combined SQL"
rm combined.sql.gz 2>/dev/null
gzip combined.sql

# Check if the container exists, and if it does, stop and remove it
if docker ps -a | grep -q 'my-postgres-container'; then
    echo "-- Restarting PostgreSQL container"
    docker stop my-postgres-container && docker rm my-postgres-container
fi

# Start PostgreSQL with combined.sql.gz and bind mount the files directory
echo "-- Starting PostgreSQL"
docker run --name my-postgres-container \
-e POSTGRES_PASSWORD=mysecretpassword \
-e POSTGRES_DB=mydatabase \
-v $(pwd)/combined.sql.gz:/docker-entrypoint-initdb.d/combined.sql.gz \
-v $(pwd)/files:/docker-entrypoint-initdb.d/files \
--health-cmd 'pg_isready -U postgres' \
--health-interval 10s \
--health-timeout 5s \
--health-retries 5 \
-p 5432:5432 \
-d postgres:latest


