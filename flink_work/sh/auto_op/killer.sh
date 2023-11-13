#!/bin/bash
### conf
HADOOP_CLASSPATH=$(hadoop classpath)
export HADOOP_CLASSPATH

yarn_app_list_path="./yarn_app_list4killer.txt"

### init
yarn application -list -appStates ALL > $yarn_app_list_path

### 每行配置包含如下内容：是否kill前询问（1/0）、正则匹配应用名
### 注意：若查找具体应用名xxx，请将参数配置为^xxx$
function killer_inner() {
  local killer_ask_switch=$1
  local app_name_pattern=$2

  local line_select=$(cat $yarn_app_list_path | awk '{print $2}' | sed 's/\[/\\[/g;s/\]/\\]/g' | grep -n "$app_name_pattern" | cut -d: -f1 | sed 's/$/p/' | tr '\n' ';')
  local line_cnt=0
  if [ -n "$line_select" ]; then
    line_cnt=$(sed -n $line_select $yarn_app_list_path | awk '{print $7}' | grep -E -v 'FINISHED|FAILED|KILLED' | wc -l)
  fi

  if [ $line_cnt -ge 1 ]; then
    local run_line_select=$(sed -n "$line_select" $yarn_app_list_path | awk '{print $7}' | grep -E -n -v 'FINISHED|FAILED|KILLED' | cut -d: -f1 | sed 's/$/p/' | tr '\n' ';')
    local app_id_rows=$(sed -n "$line_select" $yarn_app_list_path | sed -n "$run_line_select" | awk '{print $1}')
    for app_id in $app_id_rows; do
        local app_name=$(yarn application -status $app_id | grep 'Application-Name' | awk -F": " '{print $2}')
        if [ $killer_ask_switch -eq 1 ]; then
          read -p "Do you want to kill $app_name ($app_id)? (y/n): " choice
          choice=${choice:-y}
          if [ "$choice" == "y" ] || [ "$choice" == "Y" ]; then
            yarn app -kill $app_id
          fi
        else
          echo "ready to kill $app_name ($app_id)"
          yarn app -kill $app_id
        fi
    done
  else
    echo "app with name $app_name not exists"
  fi
}

function killer_without_hint() {
  killer_inner 0 "$@"
}

function killer_with_hint() {
  killer_inner 1 "$@"
}
