import com.alibaba.fastjson2.JSONObject
import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import tools._

import scala.collection.JavaConverters._

object MysqlCDCProducer {

  def main(args: Array[String]): Unit = {
    // config
    val arr = args(0).split('.')
    val (db, table, split) = (arr(0), arr(1), arr(2).toInt)
    printf("CDC DB: %s, TABLE: %s, SPLIT NO: %d\n", db, table, split)
    val conf = ConfigFactory.load("app_online.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // source def
    val serverId = conf.getString("mysql.%s.serverId".format(db))
    val username = conf.getString("mysql.username")
    val password = conf.getString("mysql.password")
    val group = conf.getConfigList("mysql.%s.instances".format(db)).asScala
    val g = group(split)
    val dbList = g.getStringList("databaseList").asScala
    val tblList = dbList.map(x => x + "." + table)
    val mysqlSource = MySqlSource.builder[JSONObject]()
      .serverId(serverId)
      .hostname(g.getString("hostname"))
      .port(g.getInt("port"))
      .databaseList(dbList: _*)
      .tableList(tblList: _*)
      .username(username)
      .password(password)
      .startupOptions(StartupOptions.initial())
      .deserializer(new MyDeserializationSchema())
      .serverTimeZone("Asia/Shanghai")
      .build()

    // sink def
    val topic = conf.getString("kafka.topic.%s".format(db))
    val sink = KafkaSink.builder()
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(topic)
        .setPartitioner(new MyShardPartitioner)
        .setKeySerializationSchema(new MyKeySerializationSchema)
        .setValueSerializationSchema(new MyValueSerializationSchema)
        .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setKafkaProducerConfig(KafkaUtils.getProducerDefaultProp(true))
      .build()

    // calc
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[JSONObject](), "Mysql CDC Source")
    src.sinkTo(sink)
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
