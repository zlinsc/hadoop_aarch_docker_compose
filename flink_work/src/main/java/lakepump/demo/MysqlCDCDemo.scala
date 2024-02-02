package lakepump.demo

import com.alibaba.fastjson2.JSONObject
import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, TopicSelector}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import lakepump.kafka.{KafkaUtils, MyKeySerializationSchema, MyShardPartitioner, MyValueSerializationSchema}
import lakepump.mysql.JsonDeserializationSchema

import scala.collection.mutable

object MysqlCDCDemo {
  //  val LOG: Logger = LoggerFactory.getLogger(getClass)
  val topic = "cdctest2"

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load("application.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //    val newFlinkConf = new Configuration()
    //    env.getConfig.getGlobalJobParameters.toMap.foreach(x => newFlinkConf.setString(x._1, x._2))
    //    newFlinkConf.setString("yarn.application.id", YarnUtils.getAppID("flinksql"))
    //    env.getConfig.setGlobalJobParameters(newFlinkConf)

    val tables = args(0) // conf.getString("mysql.table")
    val startup = if (args(1) == "init") StartupOptions.initial() else StartupOptions.specificOffset(args(1))

    //        val mysqlSource = MySqlSource.builder[String]()
    val mysqlSource = MySqlSource.builder[JSONObject]()
      .serverId(conf.getString("mysql.serverId"))
      .hostname(conf.getString("mysql.hostname"))
      .port(conf.getInt("mysql.port"))
      .databaseList(conf.getString("mysql.database"))
      .tableList(tables)
      .scanNewlyAddedTableEnabled(true)
      .username(conf.getString("mysql.username"))
      .password(conf.getString("mysql.password"))
      .startupOptions(startup)
      //      .startupOptions(StartupOptions.specificOffset("mysql-bin.000001", 5997))
      .deserializer(new JsonDeserializationSchema())
      //                        .deserializer(new JsonDebeziumDeserializationSchema())
      .serverTimeZone("Asia/Shanghai")
      //      .includeSchemaChanges(true)
      .build()

    //        val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[String](), "Mysql CDC Source")
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[JSONObject](), "Mysql CDC Source")
      .setParallelism(2).uid("src")
      .asInstanceOf[DataStream[JSONObject]]

    val sink = KafkaSink.builder()
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(topic)
        //        .setTopicSelector(new TopicSelector[JSONObject] {
        //          override def apply(t: JSONObject): String = {
        //            //    t.getString("db")+t.getString("table")
        //            topic
        //          }
        //        })
        .setPartitioner(new MyShardPartitioner)
        .setKeySerializationSchema(new MyKeySerializationSchema)
        .setValueSerializationSchema(new MyValueSerializationSchema)
        .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setTransactionalIdPrefix(System.currentTimeMillis().toString)
      .setKafkaProducerConfig(KafkaUtils.getDefaultProp(false))
      .build()

    //    src.print()
    src.sinkTo(sink).uid("sink")

    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
