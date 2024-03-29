package lakepump.kafka

import com.alibaba.fastjson2.JSONObject
import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import lakepump.hadoop.HadoopUtils
import lakepump.mysql.MyDebeziumProps
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MysqlCDC2Kafka {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val SET_STARTUP = "startup"
  val SET_DB_INSTANCE = "dbInstance"
  val SET_SERVER_ID = "serverId"
  val SET_SHARDING = "sharding"
  val SET_DB_TABLES = "dbTables"
  val SET_BUCKETS = "buckets"
  val SET_APP_NAME = "appName"

  val BINLOG_PARALLEL = 4

  def main(args: Array[String]): Unit = {
    //// extract argsMap
    println("args: " + args.mkString(";"))
    val argsMap = mutable.Map[String, String]()
    args.foreach(x => {
      val arr = x.split("=")
      if (arr.size == 2) argsMap += (arr(0) -> arr(1))
      else throw new Exception("args error: " + args.mkString(";"))
    })
    val conf = ConfigFactory.load("settings_v2_online.conf")
    val dbInstance = argsMap(SET_DB_INSTANCE)
    val sharding = argsMap(SET_SHARDING)
    val appName = argsMap(SET_APP_NAME)

    //// savepoint recover
    if (HadoopUtils.appIsRunningOrNot(appName)) throw new Exception("app of name %s is already running".format(appName))
    val configuration = new Configuration()
    val ckpDir = conf.getString("flink.checkpointDir")
    val jobIDCacheDir = conf.getString("flink.jobidCache")
    println(jobIDCacheDir + appName)
    val jobidPath = new Path(jobIDCacheDir + appName)
    val lastSavePath = HadoopUtils.getLatestValidSavepointPath(jobidPath, ckpDir)
    if (lastSavePath.nonEmpty) {
      configuration.setString("execution.savepoint.path", lastSavePath)
      println("use savepoint: " + lastSavePath)
    }
    //    val fs = HadoopUtils.fileSys
    //    if (fs.exists(jobidPath)) {
    //      val lastJobID = HadoopUtils.getFileContent(jobidPath)
    //      if (lastJobID.nonEmpty) {
    //        val lastSavePath = HadoopUtils.getNewestFile(ckpDir + lastJobID)
    //        if (lastSavePath != null) {
    //          configuration.setString("execution.savepoint.path", lastSavePath.toString)
    //          println("use savepoint: " + lastSavePath)
    //        } else println("savepoint is not found on path: " + ckpDir + lastJobID)
    //      } else println("cache file is empty")
    //    } else println("first time to create this app with name " + appName)

    //// flink config
    val env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
    env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(ckpDir)
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //// source config factory
    val startup = if (!argsMap.contains(SET_STARTUP)) {
      println("startup option is initial")
      StartupOptions.initial()
    } else {
      val specificArr = argsMap(SET_STARTUP).split("/")
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
    val serverId = argsMap(SET_SERVER_ID)
    val username = conf.getString("%s.username".format(dbInstance))
    val password = conf.getString("%s.password".format(dbInstance))
    val group = conf.getConfigList("%s.instances".format(dbInstance)).asScala
    val g = group(sharding.toInt)
    val timeZone = "Asia/Shanghai"
    val host = g.getString("hostname")
    val port = g.getInt("port")
    val dbPostfix = g.getStringList("dbPostfix").asScala
    val dbList = mutable.ArrayBuffer[String]()
    val tables = argsMap(SET_DB_TABLES) // db.table
    val tblList = tables.split(",")
    val targetTblList = ArrayBuffer[String]()
    val tblListWithPostfix = tblList.flatMap(t => {
      val arr = t.split("\\.")
      if (arr.length == 2) {
        val db = arr(0)
        val tbl = arr(1)
        val specSymb = tbl.indexOf("[") // 将acct_item_total_month_[0-9]{6}转为acct_item_total_month
        val targetTable = if (specSymb == -1) tbl else tbl.substring(0, specSymb - 1)
        targetTblList += db + "." + targetTable

        dbPostfix.map(postfix => {
          val dbPostfix = db + postfix
          dbList += dbPostfix
          dbPostfix + "." + tbl
        })
      } else throw new Exception("tables setting must be formatted like db.table")
    })
    println(tblListWithPostfix.mkString(";"))

    //// bucket config
    val buckets = argsMap(SET_BUCKETS).split(",")
    if (buckets.length != targetTblList.length) throw new Exception("buckets list size is not equal to tables list")
    val bucketMap = mutable.Map[String, Int]()
    for (i <- targetTblList.indices) bucketMap += (targetTblList(i) -> buckets(i).toInt)

    //// build source
    case class RecPack(tag: String, key: Int, row: JSONObject)
    val mysqlSource = MySqlSource.builder[RecPack]()
      .serverId(serverId)
      .hostname(host)
      .port(port)
      .databaseList(dbList: _*)
      .tableList(tblListWithPostfix: _*)
      .username(username)
      .password(password)
      .closeIdleReaders(false)
      .includeSchemaChanges(true)
      .scanNewlyAddedTableEnabled(true)
      .debeziumProperties(MyDebeziumProps.getProps)
      .startupOptions(startup)
      .serverTimeZone(timeZone)
      .deserializer(new DebeziumDeserializationSchema[RecPack] {
        override def deserialize(sourceRecord: SourceRecord, out: Collector[RecPack]): Unit = {
          val topic = sourceRecord.topic()
          val arr = topic.split("\\.")
          if (arr.length == 1) {
            LOG.warn("without handler: " + sourceRecord.toString)
            return
          }
          val jsonObj = new JSONObject()
          val db = arr(1)
          val table = arr(2)
          jsonObj.put("db", db)
          jsonObj.put("table", table)

          val key = sourceRecord.key().asInstanceOf[Struct]
          if (key != null) {
            val keyJson = new JSONObject()
            for (field <- key.schema().fields().asScala) {
              keyJson.put(field.name(), key.get(field))
            }
            jsonObj.put("key", keyJson)
          }

          val value = sourceRecord.value().asInstanceOf[Struct]
          val before = value.getStruct("before")
          if (before != null) {
            val beforeJson = new JSONObject()
            for (field <- before.schema().fields().asScala) {
              beforeJson.put(field.name(), before.get(field))
            }
            jsonObj.put("before", beforeJson)
          }
          val after = value.getStruct("after")
          if (after != null) {
            val afterJson = new JSONObject()
            for (field <- after.schema().fields().asScala) {
              afterJson.put(field.name(), after.get(field))
            }
            jsonObj.put("after", afterJson)
          }

          val opField = value.schema.field("op")
          if (opField != null) {
            val op = value.getString(opField.name)
            jsonObj.put("op", op)
          }

          val tsField = value.schema.field("ts_ms")
          if (tsField != null) {
            val ts = value.getInt64(tsField.name)
            jsonObj.put("cdc_ts", ts.toString)
          }
          jsonObj.put("prd_ts", System.currentTimeMillis().toString)

          val regex = "_\\d+$".r
          val tagName = regex.replaceAllIn(db, "") + "." + table
          val keyHash = sourceRecord.key().hashCode()

          out.collect(RecPack(tagName, keyHash, jsonObj))
        }

        override def getProducedType: TypeInformation[RecPack] = TypeInformation.of(classOf[RecPack])
      }).build()
    val rootParallel = BINLOG_PARALLEL
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[RecPack](), "Mysql CDC Source")
      .setParallelism(rootParallel).uid("src")

    //// distribute and sink table
    val tagMap = targetTblList.map(x => (x, new OutputTag[RecPack](x) {})).toMap
    val mainStream = src.process(new ProcessFunction[RecPack, RecPack] {
      override def processElement(e: RecPack, ctx: ProcessFunction[RecPack, RecPack]#Context, collector: Collector[RecPack]): Unit = {
        if (tagMap.contains(e.tag)) ctx.output(tagMap(e.tag), e)
        else {
          val regex = "_\\d+$".r
          val newTag = regex.replaceAllIn(e.tag, "")
          if (newTag != e.tag && tagMap.contains(newTag)) ctx.output(tagMap(newTag), e)
        }
      }
    }).setParallelism(rootParallel).uid("output-tag")

    //    val sink = KafkaSink.builder()
    //      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
    //        //        .setTopic(topic)
    //        .setTopicSelector(new TopicSelector[JSONObject] {
    //          override def apply(t: JSONObject): String = {
    //            val regex = "_\\d+$".r
    //            val targetDB = regex.replaceAllIn(t.getString("db"), "")
    //            val targetTable = regex.replaceAllIn(t.getString("table"), "")
    //            val topic = "t_%s_%s".format(targetDB, targetTable)
    //            topic
    //          }
    //        })
    //        .setPartitioner(new MyShardPartitioner)
    //        .setKeySerializationSchema(new MyKeySerializationSchema)
    //        .setValueSerializationSchema(new MyValueSerializationSchema)
    //        .build()
    //      )
    //      //      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    //      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    //      .setTransactionalIdPrefix(System.currentTimeMillis().toString)
    //      .setKafkaProducerConfig(KafkaUtils.getDefaultProp(true))
    //      .build()

    val srcByTagList = tagMap.map { case (x, tag) =>
      val bucketParallel = Math.min(bucketMap(x), 4)

      val srcByTag = mainStream.getSideOutput(tag).asInstanceOf[DataStream[RecPack]]
        .keyBy(new KeySelector[RecPack, Int] with ResultTypeQueryable[RecPack] {
          override def getKey(in: RecPack): Int = in.key

          override def getProducedType: TypeInformation[RecPack] = TypeInformation.of(classOf[RecPack])
        }).process(new KeyedProcessFunction[Int, RecPack, JSONObject] {
          override def processElement(in: RecPack,
                                      ctx: KeyedProcessFunction[Int, RecPack, JSONObject]#Context,
                                      out: Collector[JSONObject]): Unit = {
            out.collect(in.row)
          }
        }).setParallelism(bucketParallel).uid("dist:" + x)

      //// kafka sink
      val targetArr = x.split("\\.")
      val targetDB = targetArr(0)
      val targetTable = targetArr(1)
      val topic = "t_%s_%s".format(targetDB, targetTable)
      KafkaUtils.createTopic(topic, group.length * dbPostfix.length)
      val sink = KafkaSink.builder()
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
          .setTopic(topic)
          .setPartitioner(new MyShardPartitioner)
          .setKeySerializationSchema(new MyKeySerializationSchema)
          .setValueSerializationSchema(new MyValueSerializationSchema)
          .build()
        )
        //      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setTransactionalIdPrefix(System.currentTimeMillis().toString)
        .setKafkaProducerConfig(KafkaUtils.getDefaultProp(true))
        .build()
      srcByTag.sinkTo(sink).setParallelism(bucketParallel).uid("sink2Kafka:" + x).name(x)
      //      srcByTag.asInstanceOf[DataStream[JSONObject]]
    }
    //    srcByTagList.reduce((u1, u2) => u1.union(u2)).sinkTo(sink).setParallelism(16).uid("sink2Kafka")

    //// execute
    val jobName = getClass.getSimpleName + appName
    val jobClient = env.executeAsync(jobName)
    val jobID = jobClient.getJobID
    println("/=" + jobID)
    HadoopUtils.overwriteFileContent(jobidPath, jobID + "\n")
    jobClient.getJobExecutionResult.get()
  }
}
