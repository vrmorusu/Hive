#!/bin/bash
# Databaseline code repository
#
# Code for post: Shell Scripts to Check Data Integrity in Hive
# Base URL:      https://databaseline.bitbucket.io
# Author:        Ian Hellström
# -----------------------------------------------------------------------------
# Source Hadoop configurations and generic functions
# -----------------------------------------------------------------------------
source ../hadoop/aliases.sh
source ../functions.sh
# -----------------------------------------------------------------------------
# Generate a comma-separated list of all of a table's columns
# -----------------------------------------------------------------------------
function __hive_columns() {
  [ $# -eq 0 ] && echo "$FUNCNAME: at least one argument is required" && return 1

  local table="${1,,}"
  local exclude="$2"
  local tmpFile=desc.$$$(date +%s%N)

  # Generate list of columns and store in temporary file
  __exec_hive "SHOW COLUMNS IN $table" "--outputformat=csv2" > "$tmpFile"

  # Remove first line if it contains fields (header)
  sed -i '1{/^field$/d}' "$tmpFile"

  if [ "$exclude" != "" ]; then
    sed -i "/$exclude/d" "$tmpFile"
  fi

  # Replace newlines with commas, remove additional spaces, and remove final comma
  local cols="$(tr '\n' ',' < "$tmpFile")"
  cols="${cols//[[:space:]]/}"
  cols="${cols::-1}"

  rm "$tmpFile"

  echo "$cols"
}
# -----------------------------------------------------------------------------
# Execute a HiveQL statement (with optional beeline options)
# -----------------------------------------------------------------------------
function __exec_hive() {
  command -v beeline >/dev/null 2>&1 || { 
    echo "$FUNCNAME: beeline not available" 
    return 1 
  }

  [ $# -eq 0 ] && echo "$FUNCNAME: at least one argument is required" && return 2

  local stmt="$1"
  local opts="${2:-}"

  beeline \
    --fastConnect=true \
    --silent=true \
    "$opts" \
    -u 'jdbc:hive2://$HIVE_SERVER:$HIVE_PORT' \
    -e "$stmt"
}
# -----------------------------------------------------------------------------
# Count the number of duplicate entries for a Hive table
# -----------------------------------------------------------------------------
# Usage:   countHiveDuplicates [ columns (default: *all*) ]
# Example: countHiveDuplicates
# Example: countHiveDuplicates "col1, col2, col3"
# Example: countHiveDuplicates col1 col2 col3
function countHiveDuplicates() {
  [ $# -eq 0 ] && echo "$FUNCNAME: at least one argument is required" && return 1

  local table="${1,,}"
  local cols=""

  # Use provided list of columns or generate full list with auxiliary function
  if [ $# -gt 1 ]; then
    oldIFS=$IFS
    IFS=","
    cols="${*:2}"
    IFS=$oldIFS
  else
    cols="$(__hive_columns $table)"
  fi

  local sql="SELECT COUNT(*) AS n FROM
  (
    SELECT
      $cols
    , ROW_NUMBER() OVER (PARTITION BY $cols ORDER BY NULL) AS rn
    FROM
      $table
  ) t
  WHERE rn > 1"

  __exec_hive "$sql"
}
# -----------------------------------------------------------------------------
# Analyse the columns of a Hive table
# -----------------------------------------------------------------------------
# Usage:   analyseHiveTable schema.table [ rowLimit or rowFractionLimit [ exclude ] ]
# Example: analyseHiveTable dev.tab (all rows used, no exclusion)
# Example: analyseHiveTable dev.tab 10000 (only first 10,000 rows used)
# Example: analyseHiveTable dev.tab 0.5 (only first 50% of rows used)
# Example: analyseHiveTable dev.tab 10000 dummy (only first 10,000 rows used and
#          no statistics for columns with names that contain 'dummy')
# Example: analyseHiveTable dev.tab 0.5 dummy (only first 50% of rows used and
#          no statistics for columns with names that contain 'dummy')
# Notes:   exclude is required for tables that have more than 100-150 or so columns because
#          for each column 4 will be created (min, max, distinct_pct and null_pct), which causes post-MR exceptions:
#          java.lang.ArrayIndexOutOfBoundsException: -128
function analyseHiveTable() {
  [ $# -eq 0 ] && echo "$FUNCNAME: at least one argument is required" && return 1

  # Disable globbing of * (for queries)
  set -f

  local table="$1"
  local limitRows="${2:-}"
  local exclude="${3:-}"
  local limitClause=""

  local sqlStmt="SELECT metric_name, metric_value FROM ( SELECT MAP("

  if [ "$limitRows" != "" ]; then
    # Bash cannot handle floating-point comparisons, hence bc
    limitComparison=$(echo "$limitRows < 1 && $limitRows > 0" | bc)

    # Deal with fraction: if < 1 and > 0, count rows, and limit to % of that
    if [ "$limitComparison" = "1" ]; then
      colEcho "$FUNCNAME: computing the number of rows for $table..."
      limRows=$(__exec_hive "SELECT FLOOR($limitRows*COUNT(*)) FROM $table")
      limitRows=$(echo $limRows | cut -d'|' -f4)
    fi

    # Check that given or computed row limit > 1, otherwise do not use LIMIT clause
    limitComparison=$(echo "$limitRows > 1" | bc)
    if [ "$limitComparison" = "1" ]; then
      limitClause=" LIMIT $limitRows"
    fi
  fi

  local cols="$(__hive_columns $table $exclude)"

  # Replace each column with a MIN, MAX, COUNT-DISTINCT, and COUNT-NULL inside MAP (needed for unpivoting)
  pattern=' *\([[:alnum:]\_]*\) *,*'
  replacement="\"min_\1\",CAST(MIN(t.\1) AS STRING), \"max_\1\",CAST(MAX(t.\1) AS STRING), "
  replacement="$replacement""\"max_length_\1\",MAX(LENGTH(CAST(t.\1 AS STRING))), "
  replacement="$replacement""\"distinct_\1_pct\",ROUND(100.0*COUNT(DISTINCT t.\1)\/COUNT(*),2), "
  replacement="$replacement""\"null_\1_pct\",ROUND(100.0*SUM(CASE WHEN t.\1 IS NULL THEN 1 ELSE 0 END)\/COUNT(*),2), "
  cols="$(echo "$cols" | sed "s/$pattern/$replacement/g")"

  # Remove last two characters (i.e. final comma and white space)
  local selectList="$cols"
  selectList="${selectList%??}"

  selectList="$selectList) AS metrics_map"

  fromClause=" FROM (SELECT * FROM $table $limitClause) t ) exp "
  fromClause="$fromClause""LATERAL VIEW explode(metrics_map) mm AS metric_name, metric_value"

  # Execute query
  colEcho "$FUNCNAME: computing the metrics for $table..."
  sqlStmt="$sqlStmt""$selectList""$fromClause"

  __exec_hive "$sqlStmt"

  # Re-enable globbing of * (outside of queries)
  set +f
}