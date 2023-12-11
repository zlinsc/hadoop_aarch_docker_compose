#!/bin/bash
source /home/ads/cdc/routine.sh

routine_with_checkpoint acctdb "acct_item_total_month_[0-9]{6}" 5401-5404 dws 1 8gb
routine_with_checkpoint acctdb "order_info_log" 5405-5408 dwm 1 1gb
routine_with_checkpoint acctdb "payment" 5409-5412 bdmpTest 1 1gb
routine_with_checkpoint cust "prod_spec_inst" 5401-5404 bdmpTest 1 3gb
routine_with_checkpoint cust "PROD_SPEC_INST_ATTR" 5405-5408 bdmpTest 1 3gb
routine_with_checkpoint cust "prod_spec_inst_label_rel" 5409-5412 dwm 1 1gb
routine_with_checkpoint cust "prod_spec_res_inst" 5413-5416 bdmpTest 1 3gb
routine_with_checkpoint cust "resource_view_inst" 5417-5420 dwm 1 1gb
routine_with_checkpoint cust "resource_view_inst_attr" 5421-5424 dwm 1 1gb
routine_with_checkpoint order "INNER_ORD_OFFER_INST_PAY_INFO" 5425-5428 dwm 1 1gb
routine_with_checkpoint order "INNER_ORD_OFFER_INST_PAY_INFO_HIS" 5429-5432 bdmpTest 1 3gb
routine_with_checkpoint order "INNER_ORD_OFFER_PROD_INST_REL" 5433-5436 dwm 1 1gb
routine_with_checkpoint order "INNER_ORD_OFFER_PROD_INST_REL_HIS" 5437-5440 bdmpTest 1 3gb
routine_with_checkpoint order "INNER_ORD_PROD_SPEC_INST" 5441-5444 dwm 1 1gb
routine_with_checkpoint order "INNER_ORD_PROD_SPEC_INST_HIS" 5445-5448 bdmpTest 1 4gb
routine_with_checkpoint order "INNER_ORD_PROD_SPEC_RES_INST" 5449-5452 dwm 1 1gb
routine_with_checkpoint order "INNER_ORD_PROD_SPEC_RES_INST_HIS" 5453-5456 dws 1 8gb
routine_with_checkpoint order "MASTER_ORDER" 5457-5460 dwm 1 1gb
routine_with_checkpoint order "master_order_attr" 5461-5464 dwm 1 1gb
routine_with_checkpoint order "master_order_attr_his" 5465-5468 bdmpTest 1 3gb
routine_with_checkpoint order "MASTER_ORDER_HIS" 5469-5472 bdmpTest 1 2gb
routine_with_checkpoint order "ord_offer_inst" 5473-5476 dwm 1 1gb
routine_with_checkpoint order "ord_offer_inst_attr" 5477-5480 dwm 1 1gb
routine_with_checkpoint order "ord_offer_inst_attr_his" 5481-5484 bdmpTest 1 3gb
routine_with_checkpoint order "ord_offer_inst_his" 5485-5488 bdmpTest 1 3gb
routine_with_checkpoint order "ord_prod_spec_inst" 5489-5492 dwm 1 1gb
routine_with_checkpoint order "ord_prod_spec_inst_attr" 5493-5496 dwm 1 1gb
routine_with_checkpoint order "ord_prod_spec_inst_attr_his" 5497-5500 dws 1 8gb
routine_with_checkpoint order "ord_prod_spec_inst_his" 5501-5504 bdmpTest 1 3gb
routine_with_checkpoint order "order_attr" 5505-5508 dwm 1 1gb
routine_with_checkpoint order "order_attr_his" 5509-5512 bdmpTest 1 3gb
routine_with_checkpoint order "order_item" 5513-5516 dwm 1 1gb
routine_with_checkpoint order "order_item_attr" 5517-5520 dwm 1 1gb
routine_with_checkpoint order "order_item_attr_his" 5521-5524 dwm 1 1gb
routine_with_checkpoint order "order_item_his" 5525-5528 bdmpTest 1 3gb
routine_with_checkpoint order "order_meta" 5529-5532 dwm 1 1gb
routine_with_checkpoint order "order_meta_his" 5533-5536 bdmpTest 1 3gb
routine_with_checkpoint order "order_pay_info" 5537-5540 dwm 1 1gb
routine_with_checkpoint order "order_pay_info_his" 5541-5544 dwm 1 1gb