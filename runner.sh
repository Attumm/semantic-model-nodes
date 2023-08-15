time pypy3 main.py
#time python3 main.py
#!/bin/bash
# Combine SQL files
cat init.sql inserts.sql > start.sql

# Compress the combined SQL file
rm start.sql.gz
gzip start.sql

docker stop my-postgres-container && docker rm my-postgres-container


docker run --name my-postgres-container \
-e POSTGRES_PASSWORD=mysecretpassword \
-e POSTGRES_DB=mydatabase \
-e POSTGRES_SHARED_BUFFERS=3g \
-e POSTGRES_WORK_MEM=256MB \
-v $(pwd)/start.sql.gz:/docker-entrypoint-initdb.d/start.sql.gz \
--memory=6g \
--health-cmd 'pg_isready -U postgres' \
--health-interval 10s \
--health-timeout 5s \
--health-retries 5 \
-p 5432:5432 \
-d postgres:latest
