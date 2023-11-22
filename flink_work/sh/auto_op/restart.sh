#!/bin/bash
source /home/ads/cdc/killer.sh
source /home/ads/cdc/routine.sh

### 杀死正则匹配的任务名
#killer_with_hint "^p#acctdb...payment$"
#killer_with_hint "^p#order.7.INNER_ORD_PROD_SPEC_RES_INST_HIS$"
killer_without_hint "^p#cust...prod_spec_res_inst$"
killer_without_hint "^p#order...inner_ord_offer_prod_inst_rel_his$"
killer_without_hint "^p#order...inner_ord_prod_spec_inst_his$"
killer_without_hint "^p#order...inner_ord_prod_spec_res_inst_his$"
killer_without_hint "^p#order...order_item_his$"
killer_without_hint "^p#order...order_attr_his$"
killer_without_hint "^p#order...master_order_attr_his$"
killer_without_hint "^p#order...ord_offer_inst_attr_his$"
killer_without_hint "^p#cust...prod_spec_inst_attr$"
killer_without_hint "^p#order...ord_prod_spec_inst_attr_his$"

### 不从检查点恢复，完全重启
#routine_without_checkpoint acctdb "payment" 5409-5412 bdmpTest 2 1gb
#routine_without_checkpoint order "INNER_ORD_PROD_SPEC_RES_INST_HIS" 5453-5456 dws 1 8gb

### 从检查点恢复，用于调参及配置变更
#routine_with_checkpoint acctdb "payment" 5409-5412 bdmpTest 2 1gb
#routine_with_checkpoint order "INNER_ORD_PROD_SPEC_RES_INST_HIS" 5453-5456 dws 1 8gb