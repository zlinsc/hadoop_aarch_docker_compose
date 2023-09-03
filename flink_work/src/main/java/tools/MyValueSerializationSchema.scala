package tools

import com.alibaba.fastjson2.JSONObject
import org.apache.flink.api.common.serialization.SerializationSchema

class MyValueSerializationSchema extends SerializationSchema[JSONObject] {
  override def serialize(t: JSONObject): Array[Byte] = {
    val v = t.toString
    v.getBytes("UTF-8")
  }
}
