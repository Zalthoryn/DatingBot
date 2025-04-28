@echo off
docker-compose down
docker volume rm datingbot_postgres_data
docker-compose up -d --build
echo Waiting for PostgreSQL to start...
timeout /t 20
:check_postgres
docker exec datingbot-postgres-1 pg_isready -U Zalthor -d dating_db
if %ERRORLEVEL% neq 0 (
    echo PostgreSQL is not ready yet, waiting...
    timeout /t 5
    goto check_postgres
)
docker exec -i datingbot-postgres-1 psql -U Zalthor -d dating_db -f /docker-entrypoint-initdb.d/init.sql
echo Full database reset and initialization completed!