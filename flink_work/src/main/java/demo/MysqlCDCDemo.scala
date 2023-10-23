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
import tools.kafka.{KafkaUtils, MyKeySerializationSchema, MyShardPartitioner, MyValueSerializationSchema}
import tools.mysql.MyDeserializationSchema

/**
 * ## start app in yarn
 * flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
 * -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
 * -Dyarn.application.name=cdc_demo -c demo.MysqlCDCDemo flink_work-1.1.jar
 *
 * ## recover from checkpoint
 * flink run-application -t yarn-application -Dyarn.application.name=cdctest -s hdfs://master-node:50070/user/root/checkpoints/7a58042487da30bbc2b9cbcf28d5a2cb/chk-1 -Dclient.timeout=600s -c demo.MysqlCDCDemo flink_work-1.1.jar
 */
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
    //    val newFlinkConf = new Configuration()
    //    env.getConfig.getGlobalJobParameters.toMap.foreach(x => newFlinkConf.setString(x._1, x._2))
    //    newFlinkConf.setString("yarn.application.id", YarnUtils.getAppID("flinksql"))
    //    env.getConfig.setGlobalJobParameters(newFlinkConf)

    //    val mysqlSource = MySqlSource.builder[String]()
    val mysqlSource = MySqlSource.builder[JSONObject]()
      .serverId(conf.getString("mysql.serverId"))
      .hostname(conf.getString("mysql.hostname"))
      .port(conf.getInt("mysql.port"))
      .databaseList(conf.getString("mysql.database"))
      .tableList(conf.getString("mysql.table"))
      //      .scanNewlyAddedTableEnabled(true)
      .username(conf.getString("mysql.username"))
      .password(conf.getString("mysql.password"))
      .startupOptions(StartupOptions.initial())
      //      .startupOptions(StartupOptions.specificOffset("mysql-bin.000001", 5997))
      .deserializer(new MyDeserializationSchema())
      //                  .deserializer(new JsonDebeziumDeserializationSchema())
      //      .includeSchemaChanges(true)
      .serverTimeZone("Asia/Shanghai")
      .build()
    //    env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[String](), "Mysql CDC Source").print()

    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[JSONObject](), "Mysql CDC Source")
      .uid("src")
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
      .setTransactionalIdPrefix(System.currentTimeMillis().toString)
      .setKafkaProducerConfig(KafkaUtils.getDefaultProp(false))
      .build()

    src.print()
    src.sinkTo(sink).uid("sink")

    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
