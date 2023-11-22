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
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.util.Preconditions.checkNotNull
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig}
import org.apache.hudi.configuration.FlinkOptions
import org.apache.hudi.util.HoodiePipeline
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.{Logger, LoggerFactory}
import tools.mysqlcdc.{RowDataDeserializationRuntimeConverter, MyDebeziumProps}

import java.time.ZoneId
import scala.collection.JavaConverters._
import scala.collection.mutable

object MysqlCDC2Hudi {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val SET_STARTUP = "startup"
  val SET_LABEL = "label"
  val SET_SERVER_ID = "serverId"
  val SET_SHARDING = "sharding"
  val SET_DB_TABLES = "dbTables"
  val SET_PARALLEL_PER_TABLE = "parallelPerTable"

  def main(args: Array[String]): Unit = {
    //// flink config
    val conf = ConfigFactory.load("settings_online.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //// extract argsMap
    println("args: " + args.mkString(";"))
    val argsMap = mutable.Map[String, String]()
    args.foreach(x => {
      val arr = x.split("=")
      if (arr.size == 2) argsMap += (arr(0) -> arr(1))
      else throw new Exception("args error: " + args.mkString(";"))
    })

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
    val rootName = argsMap(SET_LABEL)
    val serverId = argsMap(SET_SERVER_ID)
    val username = conf.getString("%s.username".format(rootName))
    val password = conf.getString("%s.password".format(rootName))
    val group = conf.getConfigList("%s.instances".format(rootName)).asScala
    val sharding = argsMap(SET_SHARDING)
    val g = group(sharding.toInt)
    val timeZone = "Asia/Shanghai"
    val host = g.getString("hostname")
    val port = g.getInt("port")
    val dbPostfix = g.getStringList("dbPostfix").asScala
    val dbList = mutable.ArrayBuffer[String]()
    val tables = argsMap(SET_DB_TABLES) // db.table
    val tblList = tables.split(",")
    val tblListWithPostfix = tables.split(",").flatMap(t => {
      val arr = t.split("\\.")
      if (arr.length == 2) {
        val db = arr(0)
        val tbl = arr(1)
        dbPostfix.map(postfix => db + postfix + "." + tbl)
        dbList += db
      } else throw new Exception("tables setting must be formatted like db.table")
    })
    val tblParals = mutable.Map[String, Int]()
    if (argsMap.contains(SET_PARALLEL_PER_TABLE)) {
      val tablesParallelismArr = argsMap(SET_PARALLEL_PER_TABLE).split("\\.")
      if (tablesParallelismArr.length == tblList.length)
        tablesParallelismArr.zipWithIndex.foreach(x => tblParals += (tblList(x._2) -> x._1.toInt))
      else throw new Exception("table parallelism configs is not the same as given table num")
    } else tblList.foreach(x => tblParals += (x -> 1))

    val sourceConfigFactory = new MySqlSourceConfigFactory()
      .serverId(serverId)
      .hostname(host)
      .port(port)
      .databaseList(dbList: _*)
      .tableList(tblListWithPostfix: _*)
      .username(username)
      .password(password)
      .closeIdleReaders(false)
      .includeSchemaChanges(false)
      .debeziumProperties(MyDebeziumProps.getDebeziumProperties)
      .startupOptions(startup)
      .serverTimeZone(timeZone)

    //// obtain table schema first time
    val sourceConfig = sourceConfigFactory.createConfig(0)
    val partition: MySqlPartition = new MySqlPartition(sourceConfig.getMySqlConnectorConfig.getLogicalName)
    val jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)
    val tableSchemaMap = TableDiscoveryUtils.discoverSchemaForCapturedTables(partition, sourceConfig, jdbc).asScala
    val tableRowTypeMap = mutable.Map[String, RowType]()
    val tablePkMap = mutable.Map[String, Seq[String]]()
    tblListWithPostfix.foreach(x => {
      val arr = x.split("\\.")
      val dbTable = new TableId(arr(0), null, arr(1))
      // return RowType
      if (tableSchemaMap.contains(dbTable)) {
        val t = tableSchemaMap(dbTable).getTable
        val colsDT = t.columns().asScala.map(x => MySqlTypeUtils.fromDbzColumn(x).getLogicalType)
        colsDT += DataTypes.STRING().getLogicalType // partition column
        val rowType: RowType = RowType.of(colsDT: _*)
        // columns
        tableRowTypeMap += (x -> rowType)
        // primary key
        tablePkMap += (x -> t.primaryKeyColumnNames.asScala)
      } else throw new Exception("dbTable is not in tableSchemaMap")
    })

    //// build source
    case class RecPack(db: String, table: String, record: SourceRecord)
    val mysqlSource = MySqlSource.builder[RecPack]()
      .serverId(serverId)
      .hostname(host)
      .port(port)
      .databaseList(dbList: _*)
      .tableList(tblListWithPostfix: _*)
      .username(username)
      .password(password)
      .closeIdleReaders(false)
      .includeSchemaChanges(false)
      .debeziumProperties(MyDebeziumProps.getDebeziumProperties)
      .startupOptions(startup)
      .serverTimeZone(timeZone)
      .deserializer(new DebeziumDeserializationSchema[RecPack] {
        override def deserialize(sourceRecord: SourceRecord, out: Collector[RecPack]): Unit = {
          val topic = sourceRecord.topic()
          val arr = topic.split("\\.")
          if (arr.length != 3) {
            // todo
            LOG.warn("without db&table: " + sourceRecord.toString)
          } else {
            val db = arr(1)
            val table = arr(2)
            out.collect(RecPack(db, table, sourceRecord))
          }
        }

        override def getProducedType: TypeInformation[RecPack] = TypeInformation.of(classOf[RecPack])
      }).build()
    val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[RecPack](), "Mysql CDC Source").uid("src")

    //// distribute and sink table
    val mainStream = src.process(
      (e: RecPack, ctx: ProcessFunction[RecPack, RecPack]#Context, _: Collector[RecPack]) => {
        val regex = "_\\d+$".r
        val tagName = regex.replaceAllIn(e.db, "") + "." + e.table
        val outTag = new OutputTag[java.io.Serializable](tagName)
        ctx.output(outTag, e)
      }).uid("output-tag")

    tblList.map(tag => {
      val srcByTag = mainStream.getSideOutput(new OutputTag[java.io.Serializable](tag)).asInstanceOf[DataStream[RecPack]]
        .keyBy(new KeySelector[RecPack, Int] with ResultTypeQueryable[RecPack] {
          override def getKey(in: RecPack): Int = in.record.key().hashCode()

          override def getProducedType: TypeInformation[RecPack] = TypeInformation.of(classOf[RecPack])
        }).process(new KeyedProcessFunction[Int, RecPack, RowData] {
        override def processElement(e: RecPack,
                                    ctx: KeyedProcessFunction[Int, RecPack, RowData]#Context,
                                    out: Collector[RowData]): Unit = {
          val value = e.record.value().asInstanceOf[Struct]
          val valueSchema = e.record.valueSchema
          val opField = value.schema.field("op")
          if (opField != null) {
            val fullName = "%s.%s".format(e.db, e.table)
            val conv = RowDataDeserializationRuntimeConverter
              .createConverter(checkNotNull(tableRowTypeMap(fullName)), ZoneId.of(timeZone), null)

            val op = value.getString(opField.name)
            if (op == "c" || op == "r") {
              val after = value.getStruct(Envelope.FieldName.AFTER)
              val afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema
              out.collect(conv.convert(after, afterSchema).asInstanceOf[GenericRowData])
            } else if (op == "d") {
              val before = value.getStruct(Envelope.FieldName.BEFORE)
              val beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema
              out.collect(conv.convert(before, beforeSchema).asInstanceOf[GenericRowData])
            } else if (op == "u") {
              val before = value.getStruct(Envelope.FieldName.BEFORE)
              val beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema
              out.collect(conv.convert(before, beforeSchema).asInstanceOf[GenericRowData])

              val after = value.getStruct(Envelope.FieldName.AFTER)
              val afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema
              out.collect(conv.convert(after, afterSchema).asInstanceOf[GenericRowData])
            } else {
              LOG.error("op %s is not support".format(op))
            }
          } else {
            LOG.error("with out op: " + value.toString)
          }
        }
      }).setParallelism(tblParals.getOrElse(tag, 1)).uid("dist:" + tag)

      //// hudi sink
      val targetArr = tag.split("\\.")
      val targetDB = "hudi_%s".format(targetArr(0))
      val targetTable = targetArr(1)
      val targetHdfsPath = "%s/%s/%s".format(conf.getString("hudi.hdfsPath"), targetDB, targetTable)

      val leadTbl = "%s_01.%s".format(targetArr(0), targetTable) // use 01 database schema
      val flinkRowType = tableRowTypeMap(leadTbl)
      val schema = Schema.newBuilder().fromFields(flinkRowType.getFieldNames,
        flinkRowType.getFields.asScala.map(_.getType).map(TypeConversions.fromLogicalToDataType).asJava).build
      val options = Map(
        FlinkOptions.PATH.key() -> targetHdfsPath,
        FlinkOptions.TABLE_TYPE.key() -> HoodieTableType.MERGE_ON_READ.name(),
        FlinkOptions.PRECOMBINE_FIELD.key() -> tablePkMap(leadTbl).head,
        FlinkOptions.OPERATION.key() -> WriteOperationType.UPSERT.value(),

        FlinkOptions.HIVE_SYNC_ENABLED.key() -> "true",
        FlinkOptions.HIVE_SYNC_DB.key() -> targetDB,
        FlinkOptions.HIVE_SYNC_TABLE.key() -> targetTable,
        FlinkOptions.HIVE_SYNC_MODE.key() -> "hms",
        FlinkOptions.HIVE_SYNC_METASTORE_URIS.key() -> conf.getString("hudi.metastoreUris"),

        FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key() -> "true",
        FlinkOptions.COMPACTION_ASYNC_ENABLED.key() -> "false",
        FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key() -> "num_commits",
        FlinkOptions.COMPACTION_DELTA_COMMITS.key() -> "10",

        HoodieCleanConfig.ASYNC_CLEAN.key() -> "true",
        HoodieCleanConfig.CLEAN_MAX_COMMITS.key() -> "10",
        FlinkOptions.CLEAN_POLICY.key() -> "KEEP_LATEST_COMMITS",
        FlinkOptions.CLEAN_RETAIN_COMMITS.key() -> "10080",
        HoodieArchivalConfig.ASYNC_ARCHIVE.key() -> "true"
      ).asJava
      val hudiBuilder = HoodiePipeline.builder("%s.%s".format(targetDB, targetTable))
        .schema(schema)
        .pk(tablePkMap(leadTbl): _*)
        .partition(SET_SHARDING)
        .options(options)
      hudiBuilder.sink(srcByTag, false).uid("sink2Hudi")
    })

    //// finish
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
