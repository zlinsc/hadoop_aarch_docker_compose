#!/bin/bash
source /home/ads/cdc/killer.sh
source /home/ads/cdc/routine.sh

### 杀死正则匹配的任务名
#killer_with_hint "^p#acctdb...payment$"
#killer_with_hint "^p#order.7.INNER_ORD_PROD_SPEC_RES_INST_HIS$"

### 不从检查点恢复，完全重启
#routine_without_checkpoint acctdb "payment" 5409-5412 bdmpTest 2 1gb
#routine_without_checkpoint order "INNER_ORD_PROD_SPEC_RES_INST_HIS" 5453-5456 dws 1 8gb

### 从检查点恢复，用于调参及配置变更
#routine_with_checkpoint acctdb "payment" 5409-5412 bdmpTest 2 1gb
#routine_with_checkpoint order "INNER_ORD_PROD_SPEC_RES_INST_HIS" 5453-5456 dws 1 8gb