#!/bin/bash
# ./kill_app.sh p#cust.*.PROD_SPEC_INST_ATTR 7
if [ $# -ne 2 ]; then
  echo "Usage: $0 <task_template> <max_number>"
  exit 1
fi

TASK_TEMPLATE="$1"
MAX_NUMBER="$2"

for ((i = 0; i <= MAX_NUMBER; i++)); do
  APP_NAME="${TASK_TEMPLATE/\*/$i}"
  APP_ID=$(yarn application -list | grep "$APP_NAME" | awk '{print $1}')

  if [ -n "$APP_ID" ]; then
    echo "Killing application $APP_ID with name $APP_NAME"
    yarn application -kill "$APP_ID"
  else
    echo "No application found with name $APP_NAME"
  fi
done
