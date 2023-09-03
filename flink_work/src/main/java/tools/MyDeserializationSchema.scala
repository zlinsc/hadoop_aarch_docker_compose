package tools

import com.alibaba.fastjson2.JSONObject
import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.util.Collector
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._

class MyDeserializationSchema extends DebeziumDeserializationSchema[JSONObject] {

  @throws[Exception]
  override def deserialize(sourceRecord: SourceRecord, collector: Collector[JSONObject]): Unit = {
    val jsonObj = new JSONObject()

    val topic = sourceRecord.topic()
    val arr = topic.split("\\.")
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

    println(jsonObj.toString)
    collector.collect(jsonObj)
  }

  override def getProducedType: TypeInformation[JSONObject] = {
    Types.GENERIC(classOf[JSONObject])
  }

}
