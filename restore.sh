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

# Function to execute ClickHouse queries
execute_query() {
    local query="$1"
    local database="$2"
    local args="--host=$TARGET_HOST --port=$TARGET_PORT --user=$TARGET_USER --password=$TARGET_PASSWORD"

    if [ -n "$database" ]; then
        args="$args --database=$database"
    fi

    echo "$query" | clickhouse-client $args
}

# Function to process schema content
process_schema() {
    local content="$1"
    # Replace '\n' with actual newlines
    content=$(echo "$content" | sed 's/\\n/\n/g')
    # Fix DateTime with timezone using Perl
    content=$(echo "$content" | perl -pe 's/DateTime\((\\?'\''[^'\'']+\\?'\'')\)/DateTime($1)/g')
    # Remove any remaining backslashes
    content=$(echo "$content" | sed 's/\\//g')
    echo "$content"
}

# Check and restore each database
for db_dir in "$RESTORE_ROOT"/*; do
    if [ -d "$db_dir" ]; then
        database_name=$(basename "$db_dir")

        # Check if database exists on the target server
        EXISTS=$(execute_query "EXISTS DATABASE $database_name" "" --format=TSV)

        if [[ "$EXISTS" != "1" ]]; then
            echo "Creating database: $database_name on the target server"
            execute_query "CREATE DATABASE IF NOT EXISTS $database_name"
        else
            echo "Database $database_name already exists on the target server."
        fi

        # Directory for schema files
        SCHEMA_DIR="$db_dir/schemas"
        echo "Processing schemas in $SCHEMA_DIR"

        # Restore tables from schema files
        for schema_file in "$SCHEMA_DIR"/*.sql; do
            table_name=$(basename "$schema_file" .sql)
            echo "Creating table: $table_name in database: $database_name"

            # Read schema file, process content, and execute
            SCHEMA_CONTENT=$(cat "$schema_file")
            SCHEMA_CONTENT=$(process_schema "$SCHEMA_CONTENT")

            # Debug output for all tables
            echo "Debug: Processed SQL for $table_name:"
            echo "$SCHEMA_CONTENT"

            execute_query "$SCHEMA_CONTENT" "$database_name"
        done

        # Restore data from native files
        for table_file in "$db_dir"/*.native; do
            if [ -f "$table_file" ] && [ -s "$table_file" ]; then  # Check if file exists and is not empty
                table_name=$(basename "$table_file" .native)
                echo "Restoring data for table: $table_name in database: $database_name"

                # Check if table exists before attempting to insert data
                TABLE_EXISTS=$(execute_query "EXISTS TABLE $table_name" "$database_name" --format=TSV)

                if [[ "$TABLE_EXISTS" == "1" ]]; then
                    cat "$table_file" | clickhouse-client --host=$TARGET_HOST --port=$TARGET_PORT --user=$TARGET_USER --password=$TARGET_PASSWORD --database=$database_name --query="INSERT INTO $table_name FORMAT Native"
                else
                    echo "Table $table_name does not exist in database $database_name. Skipping data insertion."
                fi
            else
                echo "No data file or empty file for table: $table_name"
            fi
        done
    fi
done

echo "Restore process completed."