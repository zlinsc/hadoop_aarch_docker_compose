#!/bin/bash
### conf
HADOOP_CLASSPATH=$(hadoop classpath)
export HADOOP_CLASSPATH

yarn_app_list_path="./yarn_app_list_check.txt"

### init
yarn application -list -appStates ALL > $yarn_app_list_path

#http://flink-cdc-003.nm.ctdcp.com:8088/proxy/application_1682213308051_2147/jobs/03929e7ef103ff3d9a329ec1402def17/checkpoints
function check() {
  local chunk_default_size=8 # db instance default num
  local db=$1
  local table=$2

  for ((i = 0; i < $chunk_default_size; i++)); do
    local db_i_table="${db}.${i}.${table}"
    local app_name="p#${db_i_table/_\[0-9\]\{6\}/}"

    local line_select=$(cat $yarn_app_list_path | awk '{print $2}' | sed 's/\[/\\[/g;s/\]/\\]/g' | grep -n "^$app_name$" | cut -d: -f1 | sed 's/$/p/' | tr '\n' ';')
    local line_cnt=0
    if [ -n "$line_select" ]; then
      line_cnt=$(sed -n "$line_select" $yarn_app_list_path | awk '{print $7}' | grep -E -v 'FINISHED|FAILED|KILLED' | wc -l)
    fi

    if [ $line_cnt -gt 1 ]; then
      echo "app count using $app_name is more than 1 !!!"
      continue
    fi

    if [ $line_cnt -eq 1 ]; then
      echo "app using $app_name is normal"
      local run_line_select=$(sed -n "$line_select" $yarn_app_list_path | awk '{print $7}' | grep -E -n -v 'FINISHED|FAILED|KILLED' | cut -d: -f1 | sed 's/$/p/' | tr '\n' ';')
      local appid=$(sed -n "$line_select" $yarn_app_list_path | sed -n $run_line_select | awk '{print $1}')
      local jobid=$(yarn logs -applicationId $appid | grep 'Submitting Job with JobId' | awk -F"=" '{print $2}' | sed 's/.$//')
      resp=$(curl -s "http://flink-cdc-003.nm.ctdcp.com:8088/proxy/$appid/jobs/$jobid/checkpoints")
      status=$(echo "$resp" | jq -r '.latest.completed.status')
      if [ -n "$status" ]; then
        echo "$appid $jobid $status"
      else
        echo "No checkpoint status found"
      fi
    else
      echo "need to start up new app with name $app_name"
    fi
  done
}

check acctdb acct_item_total_month
check acctdb order_info_log
check acctdb payment
check cust prod_spec_inst
check cust PROD_SPEC_INST_ATTR
check cust prod_spec_inst_label_rel
check cust prod_spec_res_inst
check cust resource_view_inst
check cust resource_view_inst_attr
check order INNER_ORD_OFFER_INST_PAY_INFO
check order INNER_ORD_OFFER_INST_PAY_INFO_HIS
check order INNER_ORD_OFFER_PROD_INST_REL
check order INNER_ORD_OFFER_PROD_INST_REL_HIS
check order INNER_ORD_PROD_SPEC_INST
check order INNER_ORD_PROD_SPEC_INST_HIS
check order INNER_ORD_PROD_SPEC_RES_INST
check order INNER_ORD_PROD_SPEC_RES_INST_HIS
check order MASTER_ORDER
check order master_order_attr
check order master_order_attr_his
check order MASTER_ORDER_HIS
check order ord_offer_inst
check order ord_offer_inst_attr
check order ord_offer_inst_attr_his
check order ord_offer_inst_his
check order ord_prod_spec_inst
check order ord_prod_spec_inst_attr
check order ord_prod_spec_inst_attr_his
check order ord_prod_spec_inst_his
check order order_attr
check order order_attr_his
check order order_item
check order order_item_attr
check order order_item_attr_his
check order order_item_his
check order order_meta
check order order_meta_his
check order order_pay_info
check order order_pay_info_his