#!/usr/bin/env bash
# Usage: ./scripts/ingest_gsearch.sh <index_id> <folder>
# Example: ./scripts/ingest_gsearch.sh e74bf12a-d0dd-4d19-a965-03f4936db851 output/20260406T184006
set -euo pipefail

INDEX_ID="${1:?Usage: $0 <index_id> <folder>}"
FOLDER="${2:?Usage: $0 <index_id> <folder>}"

FILE_COUNT=$(find "$FOLDER" -name "*.json.gz" -type f | wc -l | tr -d ' ')

if [[ "$FILE_COUNT" -eq 0 ]]; then
    echo "No .json.gz files found in $FOLDER" >&2
    exit 1
fi

echo "Ingesting $FILE_COUNT files from $FOLDER to index $INDEX_ID"

while IFS= read -r FILE; do
    echo "  -> $FILE"
    gzip -dc "$FILE" | globus search ingest "$INDEX_ID" -
    sleep 7
done < <(find "$FOLDER" -name "*.json.gz" -type f | sort)

echo ""
echo "All files submitted. Waiting 30 seconds for tasks to complete..."
sleep 30

TASK_LIST_CMD="globus search task list $INDEX_ID"
echo ""
echo "To check task status again: $TASK_LIST_CMD"
echo ""
$TASK_LIST_CMD