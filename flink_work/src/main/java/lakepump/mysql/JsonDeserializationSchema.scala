package lakepump.mysql

import com.alibaba.fastjson2.JSONObject
import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.util.Collector
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
{
    "db":"order_03",
    "table":"inner_ord_prod_spec_inst",
    "key":{
        "UID":58936099
    },
    "after":{
        "ROW_ID":"d58c2ca735707909e050320a788c63b6",
        "PROD_SPEC_INST_ID":"d58c12fa306a5d2ee050320a788c47cd",
        "PROD_SPEC_ID":"85347e576aa944d3bb5166258818d630",
        "MAIN_PROD_SPEC_INST_ID":"d58c12fae6fae97ee050320a788c47c5",
        "PROD_ID":"fd2f4414e70a44ceb2566c049f075c2c",
        "OLD_ROW_ID":"",
        "USER_ID":"72697709ea764dbb975937f0aa65217e",
        "CONTRACT_ID":"",
        "ACCOUNT_ID":"5d520612647f4f55b091fe7adb5ea686",
        "ORDER_ID":"f5b49265946644bab77794ea3680700e",
        "AGENCY_ACCOUNT_ID":"",
        "INNER_ORDER_ITEM_ID":"e258c1006c7d47a3a897b4a9a0351af3",
        "IS_MASTER":"PUB_100_02_0002",
        "OPER_TYPE":"ORD_103_12_0001",
        "ORD_STATUS_CD":"PUB_100_01_0001",
        "DESCRIPTION":"",
        "STATUS_CD":"PUB_100_01_0001",
        "STATUS_DATE":"2020-09-07T21:10:15",
        "CREATE_DATE":"2020-09-07T21:10:15",
        "CREATE_STAFF":"",
        "CREATE_ORG":"",
        "UPDATE_DATE":"2020-09-07T21:10:15",
        "UPDATE_STAFF":"",
        "UPDATE_ORG":"",
        "REMARK":"",
        "VER_NUM":0,
        "OBJ_SHARDING_ID":"5d520612647f4f55b091fe7adb5ea686",
        "SHARDING_ID":"5d520612647f4f55b091fe7adb5ea686",
        "UID":58936099,
        "MASTER_ORDER_ID":"d6465ad7851c414eb49bd5fccf74fbaf",
        "PAY_TYPE":"ORD_100_11_0002",
        "RESOURCE_ID":"f76ea35bcb50407cb505c8554965efdd",
        "INSTANCE_CNT":40
    },
    "op":"r",
    "cdc_ts":"1695633595982",
    "prd_ts":"1695633598220"
}
 */
class JsonDeserializationSchema extends DebeziumDeserializationSchema[JSONObject] {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  @throws[Exception]
  override def deserialize(sourceRecord: SourceRecord, collector: Collector[JSONObject]): Unit = {
//    LOG.info(sourceRecord.toString)

    val topic = sourceRecord.topic()
    val arr = topic.split("\\.")
    if (arr.length == 1) {
      LOG.warn("without handler: " + sourceRecord.toString)
      return
    }
    val jsonObj = new JSONObject()
    jsonObj.put("db", arr(1))
    jsonObj.put("table", arr(2))

    val key = sourceRecord.key().asInstanceOf[Struct]
    if (key != null) {
      val keyJson = new JSONObject()
      for (field <- key.schema().fields().asScala) {
        keyJson.put(field.name(), key.get(field))
      }
      jsonObj.put("key", keyJson)
    }

    val value = sourceRecord.value().asInstanceOf[Struct]
    val before = value.getStruct("before")
    if (before != null) {
      val beforeJson = new JSONObject()
      for (field <- before.schema().fields().asScala) {
        beforeJson.put(field.name(), before.get(field))
      }
      jsonObj.put("before", beforeJson)
    }
    val after = value.getStruct("after")
    if (after != null) {
      val afterJson = new JSONObject()
      for (field <- after.schema().fields().asScala) {
        afterJson.put(field.name(), after.get(field))
      }
      jsonObj.put("after", afterJson)
    }

    val opField = value.schema.field("op")
    if (opField != null) {
      val op = value.getString(opField.name)
      jsonObj.put("op", op)
    }

    val tsField = value.schema.field("ts_ms")
    if (tsField != null) {
      val ts = value.getInt64(tsField.name)
      jsonObj.put("cdc_ts", ts.toString)
    }
    jsonObj.put("prd_ts", System.currentTimeMillis().toString)

    collector.collect(jsonObj)
  }

  override def getProducedType: TypeInformation[JSONObject] = {
    Types.GENERIC(classOf[JSONObject])
  }

}
