package demo

import com.alibaba.fastjson2.JSONObject
import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.{Logger, LoggerFactory}
import tools._

object MysqlCDCDemo {
//  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
//    LOG.info("starting job")
    val conf = ConfigFactory.load("application.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //    Seq(1).map(_ => {
    val mysqlSource = MySqlSource.builder[JSONObject]()
      .serverId(conf.getString("mysql.serverId"))
      .hostname(conf.getString("mysql.hostname"))
      .port(conf.getInt("mysql.port"))
      .databaseList(conf.getString("mysql.database"))
      .tableList(conf.getString("mysql.table"))
//      .scanNewlyAddedTableEnabled(true)
      .username(conf.getString("mysql.username"))
      .password(conf.getString("mysql.password"))
//      .startupOptions(StartupOptions.initial())
      .startupOptions(StartupOptions.specificOffset("mysql-bin.000001", 5997))
      .deserializer(new MyDeserializationSchema())
      //            .deserializer(new JsonDebeziumDeserializationSchema())
      //      .includeSchemaChanges(true)
      //      .splitSize(5)
      .serverTimeZone("Asia/Shanghai")
      .build()
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[JSONObject](), "Mysql CDC Source").uid("src")
      .asInstanceOf[DataStream[JSONObject]]

    val topic = "cdctest"
    val sink = KafkaSink.builder()
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(topic)
        .setPartitioner(new MyShardPartitioner)
        .setKeySerializationSchema(new MyKeySerializationSchema)
        .setValueSerializationSchema(new MyValueSerializationSchema)
        .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setKafkaProducerConfig(KafkaUtils.getProducerDefaultProp(false))
      .build()

    src.sinkTo(sink).uid("sink")
    //      src
    //    }).reduce(_.union(_)).print()

    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
