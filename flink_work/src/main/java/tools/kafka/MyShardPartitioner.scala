package tools.kafka

import com.alibaba.fastjson2.JSONObject
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.slf4j.{Logger, LoggerFactory}

class MyShardPartitioner extends FlinkKafkaPartitioner[JSONObject] {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  override def partition(record: JSONObject, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
    val i = math.abs(record.get("key").hashCode)
//    println(i % partitions.length, record.toString)
    partitions(i % partitions.length)
  }
}
