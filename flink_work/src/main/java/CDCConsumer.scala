import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import tools.KafkaUtils

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

object CDCConsumer {

  def main(args: Array[String]): Unit = {
    // config
    val arr = args(0).split('.')
    val (db, table) = (arr(0), arr(1))
    printf("CDC DB: %s, TABLE: %s\n", db, table)
    val conf = ConfigFactory.load("app_online.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // source def
    var offset = OffsetsInitializer.committedOffsets()
    if (args.length > 1) {
      val date = args(1) // "2023-08-19_22:00:00"
      val time = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss"))
      val timestamp = time.toInstant(ZoneOffset.of("+8")).toEpochMilli
      offset = OffsetsInitializer.timestamp(timestamp)
      printf("Kafka start from date: %s, offset: %d\n", date, timestamp)
    }
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(KafkaUtils.getBrokerList)
      .setTopics(conf.getString("kafka.topic.%s".format(db)))
      .setGroupId("c_mysql_cdc_group")
      .setStartingOffsets(offset)
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperties(KafkaUtils.getConsumerDefaultProp)
      .build()

    // calc
    val src = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String](), "Kafka Source")
    src.print()
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
