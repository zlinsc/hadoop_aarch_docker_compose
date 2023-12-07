package lakepump.pipeline

import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils
import com.ververica.cdc.connectors.mysql.schema.MySqlTypeUtils
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import io.debezium.connector.mysql.MySqlPartition
import io.debezium.data.Envelope
import io.debezium.relational.TableId
import lakepump.hadoop.HadoopUtils
import lakepump.mysql.{MyDebeziumProps, RowDataDeserializationRuntimeConverter, ShardDeserializationRuntimeConverterFactory}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.table.api.Schema
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.util.Preconditions.checkNotNull
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieIndexConfig, HoodieLayoutConfig}
import org.apache.hudi.configuration.FlinkOptions
import org.apache.hudi.index.HoodieIndex.{BucketIndexEngineType, IndexType}
import org.apache.hudi.table.storage.HoodieStorageLayout
import org.apache.hudi.util.HoodiePipeline
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZoneId
import scala.collection.JavaConverters._
import scala.collection.mutable

object MysqlCDC2Hudi {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val SET_STARTUP = "startup"
  val SET_DB_INSTANCE = "dbInstance"
  val SET_SERVER_ID = "serverId"
  val SET_SHARDING = "sharding"
  val SET_DB_TABLES = "dbTables"
  val SET_BUCKETS = "buckets"
  val SET_APP_NAME = "appName"

  def main(args: Array[String]): Unit = {
    //// extract argsMap
    println("args: " + args.mkString(";"))
    val argsMap = mutable.Map[String, String]()
    args.foreach(x => {
      val arr = x.split("=")
      if (arr.size == 2) argsMap += (arr(0) -> arr(1))
      else throw new Exception("args error: " + args.mkString(";"))
    })
    val conf = ConfigFactory.load("settings_online.conf")
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
    val fs = HadoopUtils.fileSys
    if (fs.exists(jobidPath)) {
      val lastJobID = HadoopUtils.getFileContent(jobidPath)
      if (lastJobID.nonEmpty) {
        val lastSavePath = HadoopUtils.getNewestFile(ckpDir + lastJobID)
        if (lastSavePath != null) {
          configuration.setString("execution.savepoint.path", lastSavePath.toString)
          println("use savepoint: " + lastSavePath)
        } else println("savepoint is not found on path: " + ckpDir + lastJobID)
      } else println("cache file is empty")
    } else println("first time to create this app with name " + appName)

    //// flink config
    val env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
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
    val tblListWithPostfix = tblList.flatMap(t => {
      val arr = t.split("\\.")
      if (arr.length == 2) {
        val db = arr(0)
        val tbl = arr(1)
        dbPostfix.map(postfix => {
          val dbPostfix = db + postfix
          dbList += dbPostfix
          dbPostfix + "." + tbl
        })
      } else throw new Exception("tables setting must be formatted like db.table")
    })

    val sourceConfigFactory = new MySqlSourceConfigFactory()
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
      .debeziumProperties(MyDebeziumProps.getDebeziumProperties)
      .startupOptions(startup)
      .serverTimeZone(timeZone)

    //// hudi bucket config
    val buckets = argsMap(SET_BUCKETS).split(",")
    if (buckets.length != tblList.length) throw new Exception("buckets list size is not equal to tables list")
    val bucketMap = mutable.Map[String, Int]()
    for (i <- 0 until tblList.size) bucketMap += (tblList(i) -> buckets(i).toInt)

    //// obtain table schema first time
    val sourceConfig = sourceConfigFactory.createConfig(0)
    val partition: MySqlPartition = new MySqlPartition(sourceConfig.getMySqlConnectorConfig.getLogicalName)
    val jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)
    val tableSchemaMap = TableDiscoveryUtils.discoverSchemaForCapturedTables(partition, sourceConfig, jdbc).asScala
    val tableRowMap = mutable.Map[String, (Seq[String], RowType)]()
    val tablePkMap = mutable.Map[String, Seq[String]]()
    tblListWithPostfix.foreach(x => {
      val arr = x.split("\\.")
      val dbTable = new TableId(arr(0), null, arr(1))
      // return RowType
      if (tableSchemaMap.contains(dbTable)) {
        val t = tableSchemaMap(dbTable).getTable
        val colsDT = t.columns().asScala.map(x => (x.name(), MySqlTypeUtils.fromDbzColumn(x).getLogicalType))
        //        colsDT += ((SET_SHARDING, DataTypes.STRING().getLogicalType)) // partition column
        val rowType = RowType.of(colsDT.map(_._2): _*)
        val colNames = colsDT.map(_._1)
        // columns
        tableRowMap += (x -> (colNames, rowType))
        // primary key
        tablePkMap += (x -> t.primaryKeyColumnNames.asScala)
      } else throw new Exception("%s is not in tableSchemaMap".format(dbTable))
    })
    println("pk=" + tablePkMap.mkString(";"))
    //    println("cols=" + tableRowMap.mkString(";"))

    //// build source
    case class RecPack(tag: String, key: Int, row: RowData)
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
      .debeziumProperties(MyDebeziumProps.getDebeziumProperties)
      .startupOptions(startup)
      .serverTimeZone(timeZone)
      .deserializer(new DebeziumDeserializationSchema[RecPack] {
        override def deserialize(sourceRecord: SourceRecord, out: Collector[RecPack]): Unit = {
          val topic = sourceRecord.topic()
          val arr = topic.split("\\.")
          if (arr.length == 1) {
            LOG.warn("without handler: " + sourceRecord.toString)
          } else if (arr.length == 3) {
            val db = arr(1)
            val table = arr(2)
            val regex = "_\\d+$".r
            val tagName = regex.replaceAllIn(db, "") + "." + table

            val keyHash = sourceRecord.key().hashCode()
            //            val bytes = MysqlCDC2Hudi.jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value())
            //            val schemaAndValue = MysqlCDC2Hudi.jsonConverter.toConnectData(sourceRecord.topic(), bytes)
            val value = sourceRecord.value().asInstanceOf[Struct]
            val valueSchema = sourceRecord.valueSchema()
            val opField = value.schema.field("op")
            if (opField != null) {
              val fullName = "%s.%s".format(db, table)
              val conv = RowDataDeserializationRuntimeConverter.createConverter(checkNotNull(tableRowMap(fullName)._2),
                ZoneId.of(timeZone), new ShardDeserializationRuntimeConverterFactory(sharding))

              val op = value.getString(opField.name)
              if (op == "c" || op == "r") {
                val after = value.getStruct(Envelope.FieldName.AFTER)
                val afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema
                val afterRowData = conv.convert(after, afterSchema).asInstanceOf[GenericRowData]
                out.collect(RecPack(tagName, keyHash, afterRowData))
              } else if (op == "d") {
                val before = value.getStruct(Envelope.FieldName.BEFORE)
                val beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema
                val beforeRowData = conv.convert(before, beforeSchema).asInstanceOf[GenericRowData]
                out.collect(RecPack(tagName, keyHash, beforeRowData))
              } else if (op == "u") {
                val before = value.getStruct(Envelope.FieldName.BEFORE)
                val beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema
                val beforeRowData = conv.convert(before, beforeSchema).asInstanceOf[GenericRowData]
                out.collect(RecPack(tagName, keyHash, beforeRowData))

                val after = value.getStruct(Envelope.FieldName.AFTER)
                val afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema
                val afterRowData = conv.convert(after, afterSchema).asInstanceOf[GenericRowData]
                out.collect(RecPack(tagName, keyHash, afterRowData))
              } else {
                LOG.error("op %s is not support".format(op))
              }
            } else {
              LOG.error("with out op: " + value.toString)
            }
          } else {
            LOG.warn("without handler: " + sourceRecord.toString)
          }
        }

        override def getProducedType: TypeInformation[RecPack] = TypeInformation.of(classOf[RecPack])
      }).build()
    val rootParallel = 4
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[RecPack](), "Mysql CDC Source")
      .setParallelism(rootParallel).uid("src")

    //// distribute and sink table
    val tagMap = tblList.map(x => (x, new OutputTag[RecPack](x) {})).toMap
    val mainStream = src.process(new ProcessFunction[RecPack, RecPack] {
      override def processElement(e: RecPack, ctx: ProcessFunction[RecPack, RecPack]#Context, collector: Collector[RecPack]): Unit = {
        if (tagMap.contains(e.tag)) ctx.output(tagMap(e.tag), e)
      }
    }).setParallelism(rootParallel).uid("output-tag")

    tagMap.map { case (x, tag) =>
      val bucketParallel = Math.min(bucketMap(x), 16)

      val srcByTag = mainStream.getSideOutput(tag).asInstanceOf[DataStream[RecPack]]
        .keyBy(new KeySelector[RecPack, Int] with ResultTypeQueryable[RecPack] {
          override def getKey(in: RecPack): Int = in.key

          override def getProducedType: TypeInformation[RecPack] = TypeInformation.of(classOf[RecPack])
        }).process(new KeyedProcessFunction[Int, RecPack, RowData] {
        override def processElement(in: RecPack,
                                    ctx: KeyedProcessFunction[Int, RecPack, RowData]#Context,
                                    out: Collector[RowData]): Unit = {
          out.collect(in.row)
        }
      }).setParallelism(bucketParallel).uid("dist:" + x)

      //// hudi sink
      val targetArr = x.split("\\.")
      val targetDB = "hudi_%s".format(targetArr(0))
      val targetTable = "%s_%s".format(targetArr(1), sharding)
      val targetHdfsPath = "%s/%s/%s".format(conf.getString("hudi.hdfsPath"), targetDB, targetTable)

      val headTbl = "%s%s.%s".format(targetArr(0), dbPostfix.head, targetArr(1)) // use first database schema
      val tblRow = tableRowMap(headTbl)
      val schema = Schema.newBuilder().fromFields(tblRow._1.asJava,
        tblRow._2.getFields.asScala.map(_.getType).map(TypeConversions.fromLogicalToDataType).asJava).build
      val options = Map(
        FlinkOptions.PATH.key() -> targetHdfsPath,
        FlinkOptions.TABLE_TYPE.key() -> HoodieTableType.MERGE_ON_READ.name(),
        FlinkOptions.PRECOMBINE_FIELD.key() -> tablePkMap(headTbl).head,
        FlinkOptions.OPERATION.key() -> WriteOperationType.UPSERT.value(),

        FlinkOptions.HIVE_SYNC_ENABLED.key() -> "true",
        FlinkOptions.HIVE_SYNC_DB.key() -> targetDB,
        FlinkOptions.HIVE_SYNC_TABLE.key() -> targetTable,
        FlinkOptions.HIVE_SYNC_MODE.key() -> "hms",
        FlinkOptions.HIVE_SYNC_METASTORE_URIS.key() -> conf.getString("hudi.metastoreUris"),
        FlinkOptions.HIVE_SYNC_CONF_DIR.key() -> "/etc/hive/conf",

        FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key() -> "true",
        FlinkOptions.COMPACTION_ASYNC_ENABLED.key() -> "false",
        FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key() -> FlinkOptions.NUM_OR_TIME,
        FlinkOptions.COMPACTION_DELTA_COMMITS.key() -> "5",
        FlinkOptions.COMPACTION_DELTA_SECONDS.key() -> "1800",

        HoodieCleanConfig.AUTO_CLEAN.key() -> "false",
        //        HoodieCleanConfig.ASYNC_CLEAN.key() -> "true",
        //        HoodieCleanConfig.CLEAN_MAX_COMMITS.key() -> "10",
        //        FlinkOptions.CLEAN_POLICY.key() -> "KEEP_LATEST_COMMITS",
        //        FlinkOptions.CLEAN_RETAIN_COMMITS.key() -> "10080",
        HoodieArchivalConfig.ASYNC_ARCHIVE.key() -> "true",

        HoodieIndexConfig.INDEX_TYPE.key() -> IndexType.BUCKET.name,
        HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key() -> BucketIndexEngineType.SIMPLE.name(),
        HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key() -> bucketMap(x).toString,
        HoodieLayoutConfig.LAYOUT_TYPE.key() -> HoodieStorageLayout.LayoutType.BUCKET.name,
        HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME.key() -> HoodieLayoutConfig.SIMPLE_BUCKET_LAYOUT_PARTITIONER_CLASS_NAME,

        FlinkOptions.WRITE_TASKS.key() -> bucketParallel.toString,
        FlinkOptions.BUCKET_ASSIGN_TASKS.key() -> bucketParallel.toString,
      ).asJava
      val hudiBuilder = HoodiePipeline.builder("%s_%s".format(targetDB, targetTable))
        .schema(schema)
        .pk(tablePkMap(headTbl): _*)
        //        .partition(SET_SHARDING)
        .options(options)
      hudiBuilder.sink(srcByTag, false).uid("sink2Hudi:" + x)
    }

    //// execute
    val jobName = getClass.getSimpleName + appName
    val jobClient = env.executeAsync(jobName)
    val jobID = jobClient.getJobID
    println("/=" + jobID)
    HadoopUtils.overwriteFileContent(jobidPath, jobID + "\n")
    jobClient.getJobExecutionResult.get()
  }
}
