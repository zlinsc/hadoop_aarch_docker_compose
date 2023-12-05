package lakepump.cdc

import com.alibaba.fastjson2.JSONObject
import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import lakepump.kafka.KafkaUtils.getDefaultProp
import lakepump.kafka.{KafkaUtils, MyKeySerializationSchema, MyShardPartitioner, MyValueSerializationSchema}
import lakepump.mysql.{JsonDeserializationSchema, MyDebeziumProps}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import scala.collection.JavaConverters._

object MysqlCDCProducer {

  def main(args: Array[String]): Unit = {
    //// init
    //// args: db.i.table serverId gtid/6dc8b5af-1616-11ec-8f60-a4ae12fe8402:1-20083670
    println("args: " + args.mkString(";"))
    val arr = args(0).split('.')
    val (db, chunk, table) = (arr(0), arr(1).toInt, arr(2))
    printf("CDC DB: %s, TABLE: %s, CHUNK NO: %d\n", db, table, chunk)
    val conf = ConfigFactory.load("app_online.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //// source
    val startup = if (!args.last.contains("/")) {
      println("startup option is initial")
      StartupOptions.initial()
    } else {
      val specificArr = args.last.split("/")
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
    val serverId = args(1)
    val username = conf.getString("%s.username".format(rootName))
    val password = conf.getString("%s.password".format(rootName))
    val group = conf.getConfigList("%s.instances".format(rootName)).asScala
    val g = group(chunk)
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
      .closeIdleReaders(true)
      .debeziumProperties(MyDebeziumProps.getDebeziumProperties)
      .startupOptions(startup)
      .deserializer(new JsonDeserializationSchema())
      .serverTimeZone("Asia/Shanghai")
      .build()

    //// sink
    //    val topic = conf.getString("kafka.topic.%s".format(db))
    //// 将acct_item_total_month_[0-9]{6}转为acct_item_total_month
    val specSymb = table.indexOf("[")
    val tableNew = if (specSymb == -1) table else table.substring(0, specSymb - 1)
    val topic = "t_%s_%s".format(db.toLowerCase, tableNew.toLowerCase)
    val adminClient = AdminClient.create(getDefaultProp(true))
    val listTopics = adminClient.listTopics()
    if (listTopics.names().get().contains(topic)) {
      printf("topic existed: %s\n", topic)
    } else {
      val partitionCount = group.length * dbList.length
      val replicationFactor: Short = 3
      val newTopic = new NewTopic(topic, partitionCount, replicationFactor)
      val createRslt = adminClient.createTopics(Set(newTopic).asJava)
      createRslt.values().get(topic)
      printf("topic created successfully: %s\n", topic)
    }
    adminClient.close()
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
      .setKafkaProducerConfig(KafkaUtils.getDefaultProp(true))
      .build()

    //// calc
    env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[JSONObject](), "Mysql CDC Source")
      .uid("src")
      .sinkTo(sink)
      .uid("sink")
    env.execute("p#%s.%d.%s".format(db, chunk, table))
  }
}
