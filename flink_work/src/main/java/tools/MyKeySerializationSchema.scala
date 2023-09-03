package tools

import com.alibaba.fastjson2.JSONObject
import org.apache.flink.api.common.serialization.SerializationSchema

class MyKeySerializationSchema extends SerializationSchema[JSONObject] {
  override def serialize(t: JSONObject): Array[Byte] = {
    val key = t.get("key").asInstanceOf[JSONObject]
    val it = key.keySet().iterator()
    val k = if (it.hasNext) key.get(it.next()).asInstanceOf[String] else ""
    k.getBytes("UTF-8")
  }
}
