@echo off
docker-compose down
docker volume rm datingbot_postgres_data
docker-compose up -d --build
echo Waiting for PostgreSQL to start...
timeout /t 10
docker exec -i datingbot-postgres-1 psql -U <your_user> -d dating_db -f /docker-entrypoint-initdb.d/init.sql
echo Full database reset and initialization completed!