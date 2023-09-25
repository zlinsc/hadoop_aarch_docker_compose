package tools.kafka

import com.alibaba.fastjson2.JSONObject
import org.apache.flink.api.common.serialization.SerializationSchema

class MyValueSerializationSchema extends SerializationSchema[JSONObject] {
  override def serialize(t: JSONObject): Array[Byte] = {
    val v = t.toString
    println(v)
    v.getBytes("UTF-8")
  }
}
