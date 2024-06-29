#!/bin/bash

# ClickHouse server connection details
HOST="IP_ADDRESS"
PORT="9000"
USER="default"
PASSWORD="PASSWORD"

# Root directory for the backup output
OUTPUT_ROOT="/data/backup/bckp"

# Generate a date-based folder name for the backup
BACKUP_FOLDER="$(date +"%Y-%m-%d_%H-%M-%S")"
BACKUP_ROOT="$OUTPUT_ROOT/$BACKUP_FOLDER"

# Create the root backup directory
mkdir -p "$BACKUP_ROOT"

# List all databases
DATABASES=$(clickhouse-client --host="$HOST" --port="$PORT" --user="$USER" --password="$PASSWORD" --query="SHOW DATABASES FORMAT TSV" )
#DATABASES=apifonica_events

# Iterate over each database
while IFS=$'\t' read -r database_name; do
    echo "Processing database: $database_name"

    # Skip system databases or any other databases you don't want to back up
    if [[ "$database_name" == "system" ]]; then
        continue
    fi

    # Set output directory for the current database
    OUTPUT_DIR="$BACKUP_ROOT/$database_name"

    # Create the output directory
    mkdir -p "$OUTPUT_DIR"

    # Directory for schemas within the database directory
    SCHEMA_DIR="$OUTPUT_DIR/schemas"
    mkdir -p "$SCHEMA_DIR"

    # Get the list of tables from the current database
    TABLES=$(clickhouse-client --host="$HOST" --port="$PORT" --user="$USER" --password="$PASSWORD" --database="$database_name" --query="SHOW TABLES" --format="TabSeparated")

    # Iterate over each table to backup data and schema
    while IFS= read -r table_name; do
        if [ -z "$table_name" ]; then
            continue
        fi

        echo "Backing up table: $table_name from database: $database_name"

        # Save the schema of the table
        echo "Saving schema for table: $table_name"
        clickhouse-client --host="$HOST" --port="$PORT" --user="$USER" --password="$PASSWORD" --database="$database_name" --query="SHOW CREATE TABLE \`$table_name\`" > "$SCHEMA_DIR/$table_name.sql"

        # Export the data in native format
        echo "Exporting data for table: $table_name"
        clickhouse-client --host="$HOST" --port="$PORT" --user="$USER" --password="$PASSWORD" --database="$database_name" --query="SELECT * FROM \`$table_name\` FORMAT Native" > "$OUTPUT_DIR/$table_name.native"
    done <<< "$TABLES"

done <<< "$DATABASES"

# Create a tar.gz archive of the backup folder
ARCHIVE_FILE="$OUTPUT_ROOT/${BACKUP_FOLDER}.tar.gz"
tar -czf "$ARCHIVE_FILE" -C "$OUTPUT_ROOT" "$BACKUP_FOLDER"

echo "Backup process completed."