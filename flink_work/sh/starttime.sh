#!/bin/bash
function get_start_time() {
  if [ $# -ne 1 ]; then
    echo "Usage: $0 <task_template>"
    exit 1
  fi

  APP_NAME=$1
  APP_ID=$(yarn application -list | grep -E "\b$APP_NAME\b" | awk '{print $1}')

  if [ -n "$APP_ID" ]; then
    START_TIME=$(yarn application -status $APP_ID | grep "Start-Time" | awk '{print $3}')
    timestamp_s=$((START_TIME / 1000))
    formatted_date=$(TZ='Asia/Shanghai' date -d "@$timestamp_s" "+%Y-%m-%d %H:%M:%S")
    formatted_string=$(echo "$APP_NAME,$formatted_date" | sed 's/^p#//' | sed 's/\.0\./\./')
    echo $formatted_string
  else
    echo "No application found with name $APP_NAME"
  fi
}
