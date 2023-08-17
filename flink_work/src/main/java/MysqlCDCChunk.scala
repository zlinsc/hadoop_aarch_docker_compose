import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.JavaConverters._

object MysqlCDCChunk {

  def main(args: Array[String]): Unit = {
    // config
    val arr = args(0).split(".")
    val (db, table, split) = (arr(0), arr(1), arr(2).toInt)
    printf("CDC DB: %s, TABLE: %s, SPLIT NO: %d\n", db, table, split)
    val conf = ConfigFactory.load("app_online.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    env.getCheckpointConfig.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    env.enableCheckpointing(60000)

    // source def
    val serverId = conf.getString("mysql.%s.serverId".format(db))
    val username = conf.getString("mysql.username")
    val password = conf.getString("mysql.password")
    val group = conf.getConfigList("mysql.%s.instances".format(db)).asScala
    val g = group(split)
    val dbList = g.getStringList("databaseList").asScala
    val tblList = dbList.map(x => x + "." + table)
    val mysqlSource = MySqlSource.builder[String]()
      .serverId(serverId)
      .hostname(g.getString("hostname"))
      .port(g.getInt("port"))
      .databaseList(dbList: _*)
      .tableList(tblList: _*)
      .username(username)
      .password(password)
      .startupOptions(StartupOptions.initial())
      .deserializer(new JsonDebeziumDeserializationSchema())
      //      .includeSchemaChanges(true)
      //      .splitSize(5)
      .build()

    // sink def
    val topic = conf.getString("kafka.topic")
    val sink = KafkaSink.builder()
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(topic)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setKafkaProducerConfig(KafkaProp.getProducerDefaultProp())
      .build()

    // calc
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[String](), "Mysql CDC Source")
    src.sinkTo(sink)
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
