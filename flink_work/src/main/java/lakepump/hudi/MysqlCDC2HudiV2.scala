package lakepump.hudi

import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils
import com.ververica.cdc.connectors.mysql.schema.MySqlTypeUtils
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import io.debezium.connector.mysql.MySqlPartition
import io.debezium.relational.TableId
import lakepump.hadoop.HadoopUtils
import lakepump.mysql.{MyDebeziumProps, MySqlSourceDef, RecPack, RecPackDeserializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieIndexConfig, HoodieLayoutConfig}
import org.apache.hudi.configuration.FlinkOptions
import org.apache.hudi.index.HoodieIndex.{BucketIndexEngineType, IndexType}
import org.apache.hudi.table.storage.HoodieStorageLayout
import org.apache.hudi.util.HoodiePipeline
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object MysqlCDC2HudiV2 {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  // todo specify environment
  val ENV = "dev"
  val CONF_FILE = if (ENV.equals("dev")) "application.conf" else "settings_v2_online.conf"

  val SYS_APP_NAME = "yarn.application.name"

  val SET_DB_INSTANCE = "db.instance"
  val SET_SHARDING = "sharding"
  val SET_SERVER_ID = "server.id"
  val SET_DB_TABLES = "db.tables"
  val SET_BUCKETS = "buckets"

  val OPT_RECOVER_JOB_ID = "recover.job.id"

  val SHARDING_COL = "_hudi_sharding"
  val WRITE_TIME_COL = "_hudi_write_time"

  val MAX_BUCKET_PER_TABLE = 16

  def main(args: Array[String]): Unit = {
    /** load configs */
    val argParams = ParameterTool.fromArgs(args)
    println("argParams: " + argParams.toMap.toString)
    val conf = ConfigFactory.load(CONF_FILE)
    val envConf = StreamExecutionEnvironment.getExecutionEnvironment.getConfiguration.asInstanceOf[Configuration]

    /** flink config */
    /* app is already running or not */
    val appName = envConf.toMap.get(SYS_APP_NAME)
    println("appName: " + appName)
    if (HadoopUtils.appIsRunningOrNot(appName))
      throw new Exception("app of name %s is already running".format(appName))

    /* return to specified job or the lastest one */
    val ckpDir = conf.getString("flink.checkpointDir")
    val jobIDCacheDir = conf.getString("flink.jobidCache")
    val app2jobPath = new Path(jobIDCacheDir + appName)
    val recoverJobID = argParams.get(OPT_RECOVER_JOB_ID, "")
    val lastSavePath = HadoopUtils.getLatestValidSavepointPathV2(app2jobPath, recoverJobID, ckpDir)

    /* checkpoint setting */
    val configuration = new Configuration()
    if (lastSavePath.nonEmpty) {
      configuration.setString("execution.savepoint.path", lastSavePath)
      println("use savepoint: " + lastSavePath)
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
    val ckpInterval = conf.getInt("flink.checkpointInterval")
    env.enableCheckpointing(ckpInterval, CheckpointingMode.AT_LEAST_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(ckpDir)
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /** data source config */
    /* source settings */
    val timeZone = "Asia/Shanghai"
    val startup = StartupOptions.initial()
    val dbInstance = argParams.get(SET_DB_INSTANCE)
    val sharding = argParams.getInt(SET_SHARDING)
    val serverId = argParams.get(SET_SERVER_ID)
    val binlogParallel = if (serverId.contains("-")) {
      val arr = serverId.split("-")
      arr(1).toInt - arr(0).toInt + 1
    } else 1
    val username = conf.getString("%s.username".format(dbInstance))
    val password = conf.getString("%s.password".format(dbInstance))
    val group = conf.getConfigList("%s.instances".format(dbInstance)).asScala
    val g = group(sharding)
    val host = g.getString("hostname")
    val port = g.getInt("port")
    val dbPostfix = g.getStringList("dbPostfix").asScala
    val dbList = mutable.ArrayBuffer[String]()
    val tables = argParams.get(SET_DB_TABLES) // db.table
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

    /* obtain table schema */
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
      .debeziumProperties(MyDebeziumProps.getHudiProps)
      .startupOptions(startup)
      .serverTimeZone(timeZone)
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
        val colsDT = (WRITE_TIME_COL, DataTypes.STRING().getLogicalType) +: // write time column
          (SHARDING_COL, DataTypes.STRING().getLogicalType) +: // sharding column
          t.columns().asScala.map(x => (x.name(), MySqlTypeUtils.fromDbzColumn(x).getLogicalType))
        val rowType = RowType.of(colsDT.map(_._2): _*)
        val colNames = colsDT.map(_._1)
        // columns
        tableRowMap += (x -> (colNames, rowType))
        // primary key
        val lst = t.primaryKeyColumnNames.asScala :+ SHARDING_COL
        tablePkMap += (x -> lst)
      } else throw new Exception("%s is not in tableSchemaMap".format(dbTable))
    })
    println("pk=" + tablePkMap.mkString(";"))
    //    println("cols=" + tableRowMap.mkString(";"))

    /* bucket parallelism setting */
    val buckets = argParams.get(SET_BUCKETS).split(",")
    if (buckets.length != tblList.length) throw new Exception("buckets list size is not equal to tables list")
    val bucketMap = mutable.Map[String, Int]()
    for (i <- tblList.indices) bucketMap += (tblList(i) -> buckets(i).toInt)

    /** build data source */
    val deserializer = new RecPackDeserializationSchema(sharding, tableRowMap.toMap, ENV)
    val mysqlSource = MySqlSourceDef.newInstance(sourceConfigFactory, deserializer)
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[RecPack](), "Mysql CDC Source")
      .setParallelism(binlogParallel).uid("src")

    /** distribute tables */
    val tagMap = tblList.map(x => (x, new OutputTag[RecPack](x) {})).toMap
    val mainStream = src.process(new ProcessFunction[RecPack, RecPack] {
      override def processElement(e: RecPack,
                                  ctx: ProcessFunction[RecPack, RecPack]#Context,
                                  collector: Collector[RecPack]): Unit = {
        if (tagMap.contains(e.tag)) ctx.output(tagMap(e.tag), e)
      }
    }).setParallelism(binlogParallel).uid("output-tag")

    /** sink to hudi */
    tagMap.map { case (x, tag) =>
      val bucketParallel = Math.min(bucketMap(x), MAX_BUCKET_PER_TABLE)

      /* partition by primary key */
      val srcByTag = mainStream.getSideOutput(tag).asInstanceOf[DataStream[RecPack]]
        .keyBy(new KeySelector[RecPack, Int] with ResultTypeQueryable[RecPack] {
          override def getKey(in: RecPack): Int = in.pk

          override def getProducedType: TypeInformation[RecPack] = TypeInformation.of(classOf[RecPack])
        }).process(new KeyedProcessFunction[Int, RecPack, RowData] {
          override def processElement(in: RecPack,
                                      ctx: KeyedProcessFunction[Int, RecPack, RowData]#Context,
                                      out: Collector[RowData]): Unit = {
            out.collect(in.row)
          }
        }).setParallelism(bucketParallel).uid("dist:" + x)

      /* hudi sink config */
      val targetArr = x.split("\\.")
      val targetDB = "hudi_%s".format(targetArr(0))
      val targetTable = "%s_%d".format(targetArr(1), sharding)
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
        FlinkOptions.HIVE_SYNC_CONF_DIR.key() -> conf.getString("hudi.hiveConfPath"),

        FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key() -> "true",
        FlinkOptions.COMPACTION_ASYNC_ENABLED.key() -> "false",
        FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key() -> FlinkOptions.NUM_OR_TIME,
        FlinkOptions.COMPACTION_DELTA_COMMITS.key() -> "5",
        FlinkOptions.COMPACTION_DELTA_SECONDS.key() -> "1800",

        HoodieCleanConfig.AUTO_CLEAN.key() -> "false",
        HoodieArchivalConfig.ASYNC_ARCHIVE.key() -> "true",
        FlinkOptions.CLEAN_ASYNC_ENABLED.key() -> "false",
        FlinkOptions.CLEAN_POLICY.key() -> "KEEP_LATEST_COMMITS",
        FlinkOptions.CLEAN_RETAIN_COMMITS.key() -> "10080",

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
      hudiBuilder.sink(srcByTag, false).setParallelism(bucketParallel).uid("sink2Hudi:" + x)
    }

    /** execute job */
    val jobName = getClass.getSimpleName + appName
    val jobClient = env.executeAsync(jobName)
    val jobID = jobClient.getJobID.toString
    breakable(
      while (true) {
        Thread.sleep(ckpInterval)
        if (HadoopUtils.existValidCheckpointPath(jobID, ckpDir)) break
      }
    )
    HadoopUtils.savAppName2JobID(app2jobPath, jobID)
    jobClient.getJobExecutionResult.get()
  }
}
