#!/bin/bash

# ClickHouse server connection details for the target cluster
TARGET_HOST="IP_ADDRESS"
TARGET_PORT="9000"
TARGET_USER="default"
TARGET_PASSWORD="PASSWORD"

# Root directory where the backup archives are stored
BACKUP_ROOT="/data/restore/"

# Specify the backup folder to restore from
BACKUP_FOLDER="2024-04-23_08-04-45-1"


if [ -z "$BACKUP_FOLDER" ]; then
    echo "Backup folder not specified."
    exit 1
fi

RESTORE_ROOT="$BACKUP_ROOT/$BACKUP_FOLDER"

# Ensure the backup directory exists
if [ ! -d "$RESTORE_ROOT" ]; then
    echo "Backup directory does not exist: $RESTORE_ROOT"
    exit 1
fi

# Check and restore each database
for db_dir in "$RESTORE_ROOT"/*; do
    if [ -d "$db_dir" ]; then
        database_name=$(basename "$db_dir")

        # Check if database exists on the target server
        EXISTS=$(clickhouse-client --host="$TARGET_HOST" --port="$TARGET_PORT" --user="$TARGET_USER" --password="$TARGET_PASSWORD" --query="EXISTS DATABASE $database_name" --format=TSV)

        if [[ "$EXISTS" == "1" ]]; then
            echo "Database $database_name already exists on the target server. Skipping..."
            continue
        fi

        # Create database on the target server
        echo "Creating database: $database_name on the target server"
        clickhouse-client --host="$TARGET_HOST" --port="$TARGET_PORT" --user="$TARGET_USER" --password="$TARGET_PASSWORD" --query="CREATE DATABASE IF NOT EXISTS $database_name"

        # Directory for schema files
        SCHEMA_DIR="$db_dir/schemas"

        # Restore tables from schema files
        for schema_file in "$SCHEMA_DIR"/*.sql; do
          table_name=$(basename "$schema_file" .sql)
          echo "Creating table: $table_name in database: $database_name"

          # Read schema file and remove backslashes using sed
' | sed 's/\\n//g' | sed 's/\\//g' )ema_file" | tr -d '

          # Execute clickhouse-client with processed schema content
          clickhouse-client --host="$TARGET_HOST" --port="$TARGET_PORT" --user="$TARGET_USER" --password="$TARGET_PASSWORD" --database="$database_name" --multiquery --query="$SCHEMA_CONTENT"
        done
        # Restore data from native files
        for table_file in "$db_dir"/*.native; do
            if [ -f "$table_file" ] && [ -s "$table_file" ]; then  # Check if file exists and is not empty
                table_name=$(basename "$table_file" .native)
                echo "Restoring table: $table_name to database: $database_name"
                clickhouse-client --host="$TARGET_HOST" --port="$TARGET_PORT" --user="$TARGET_USER" --password="$TARGET_PASSWORD" --database="$database_name" --query="INSERT INTO $table_name FORMAT Native" < "$table_file"
            else
                echo "No data to insert for table: $table_name from file: $table_file"
            fi
        done
    fi
done

echo "Restore process completed."