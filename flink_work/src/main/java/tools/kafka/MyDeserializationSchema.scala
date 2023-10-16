package tools.kafka

import com.alibaba.fastjson2.JSONObject
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

class MyDeserializationSchema extends KafkaRecordDeserializationSchema[String] {

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], collector: Collector[String]): Unit = {
//    val par = consumerRecord.partition()
    val r = new String(consumerRecord.value())
    val jsonObj = JSONObject.parseObject(r)
//    val tbl = jsonObj.getOrDefault("table", "").toString.toUpperCase
    val key = jsonObj.getOrDefault("key", "").toString
    //    val p = par + "," + tbl
    val p = key + "###" + r
    collector.collect(p)
  }

  override def getProducedType: TypeInformation[String] = {
    Types.STRING
  }
}
