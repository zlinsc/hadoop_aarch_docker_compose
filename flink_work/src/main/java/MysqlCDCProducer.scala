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
    // init
    println("args: " + args.mkString(";"))
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
    val startup = if (args.length < 2) {
      println("startup option is initial")
      StartupOptions.initial()
    } else {
      val specificArr = args(1).split("/")
      specificArr(0) match {
        case "pos" =>
          // pos/mysql-bin.000001:5997
          println("startup option is position, specific str is: " + specificArr(1))
          val offsetArr = specificArr(1).split(":")
          StartupOptions.specificOffset(offsetArr(0), offsetArr(1).toLong)
        case "gtid" =>
          // gtid/6dc8b5af-1616-11ec-8f60-a4ae12fe8402:1-20083670,6de8242f-1616-11ec-94a2-a4ae12fe9796:1-700110909
          println("startup option is gtid, specific str is: " + specificArr(1))
          StartupOptions.specificOffset(specificArr(1).trim)
        case _ =>
          throw new Exception("startup option is not support")
      }
    }
    val rootName = "mysql_%s".format(db)
    val serverId = conf.getString("%s.serverId".format(rootName))
    val username = conf.getString("%s.username".format(rootName))
    val password = conf.getString("%s.password".format(rootName))
    val group = conf.getConfigList("%s.instances".format(rootName)).asScala
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
      .startupOptions(startup)
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
      .setTransactionalIdPrefix(System.currentTimeMillis().toString)
      .setKafkaProducerConfig(KafkaUtils.getProducerDefaultProp(true))
      .build()

    // calc
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[JSONObject](), "Mysql CDC Source")
      .uid("src")
    src.sinkTo(sink)
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
