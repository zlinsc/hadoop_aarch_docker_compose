package lakepump.demo

import com.alibaba.fastjson2.JSONObject
import com.typesafe.config.ConfigFactory
import lakepump.cdc.RowUtils
import lakepump.kafka.KafkaUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.types.RowKind
import org.apache.flink.util.Collector
import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy
import org.apache.hudi.common.config.LockConfiguration
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.config._
import org.apache.hudi.configuration.FlinkOptions
import org.apache.hudi.hive.transaction.lock.HiveMetastoreBasedLockProvider
import org.apache.hudi.index.HoodieIndex.BucketIndexEngineType
import org.apache.hudi.index.HoodieIndex.IndexType.BUCKET
import org.apache.hudi.table.storage.HoodieStorageLayout
import org.apache.hudi.util.HoodiePipeline
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object HudiDemo {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load("application.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // todo mysql definition
    val arrColName = ArrayBuffer[String]()
    val arrColType = ArrayBuffer[String]()
    val arrColPK = ArrayBuffer[String]()
    Class.forName("com.mysql.cj.jdbc.Driver")
    val db = "test_db"
    val table = "cdc_order"
    val url = "jdbc:mysql://db-node:3306/%s".format(db)
    val user = "root"
    val password = "123456"
    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, user, password)
      val statement = connection.createStatement()
      val rs: ResultSet = statement.executeQuery("desc %s.%s".format(db, table))
      while (rs.next()) {
        val colName = rs.getString("Field")
        arrColName += colName
        val colType = rs.getString("Type")
        arrColType += colType
        if (rs.getString("Key").equals("PRI")) arrColPK += colName
      }
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
    println("arrColName: " + arrColName.mkString(","))
    println("arrColType: " + arrColType.mkString(","))
    println("arrColPK: " + arrColPK.mkString(","))

    // kafka source
    val kafkaSource = KafkaSource.builder[RowData]()
      .setBootstrapServers(KafkaUtils.getBrokerList)
      .setTopics(MysqlCDCDemo.topic)
      .setGroupId("c_mysql_cdc_group")
      .setDeserializer(new KafkaRecordDeserializationSchema[RowData] {
        override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], collector: Collector[RowData]): Unit = {
          val r = new String(consumerRecord.value())
          val jsonObj = JSONObject.parseObject(r)
          val op = jsonObj.getOrDefault("op", "")
          op match {
            case "r" | "c" =>
              val row = new GenericRowData(arrColName.length)
              row.setRowKind(RowKind.INSERT)
              val after = jsonObj.getJSONObject("after")
              arrColName.zipWithIndex.foreach { case (colName, i) =>
                row.setField(i, RowUtils.convertValue(after.get(colName), RowUtils.transMySQLColType(arrColType(i))))
              }
              collector.collect(row)

            case "u" =>
              val brow = new GenericRowData(arrColName.length)
              brow.setRowKind(RowKind.UPDATE_BEFORE)
              val before = jsonObj.getJSONObject("before")
              arrColName.zipWithIndex.foreach { case (colName, i) =>
                brow.setField(i, RowUtils.convertValue(before.get(colName), RowUtils.transMySQLColType(arrColType(i))))
              }
              collector.collect(brow)

              val arow = new GenericRowData(arrColName.length)
              arow.setRowKind(RowKind.UPDATE_AFTER)
              val after = jsonObj.getJSONObject("after")
              arrColName.zipWithIndex.foreach { case (colName, i) =>
                arow.setField(i, RowUtils.convertValue(after.get(colName), RowUtils.transMySQLColType(arrColType(i))))
              }
              collector.collect(arow)

            case "d" =>
              val row = new GenericRowData(arrColName.length)
              row.setRowKind(RowKind.DELETE)
              val before = jsonObj.getJSONObject("before")
              arrColName.zipWithIndex.foreach { case (colName, i) =>
                row.setField(i, RowUtils.convertValue(before.get(colName), RowUtils.transMySQLColType(arrColType(i))))
              }
              collector.collect(row)
          }
        }

        override def getProducedType: TypeInformation[RowData] = {
          TypeInformation.of(classOf[RowData])
        }
      })
      //            .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperties(KafkaUtils.getDefaultProp(false))
      .build()
    val src = env
      .fromSource(kafkaSource, WatermarkStrategy.noWatermarks[RowData](), "Kafka Source")
      .uid("src")

    // hudi sink
    val arrHudiColType = arrColType.map(t => RowUtils.transMySQLColType(t)).asJava
    val hudiSchema = Schema.newBuilder().fromFields(arrColName.asJava, arrHudiColType).build()
    val schema = Schema.newBuilder
      .fromColumns(hudiSchema.getColumns)
      .primaryKey(arrColPK: _*)
      .build
    val options = Map(
      FlinkOptions.PATH.key() -> "hdfs://master-node:50070/tmp/cdc_order_hudi",
      FlinkOptions.TABLE_TYPE.key() -> HoodieTableType.MERGE_ON_READ.name(),
      //      FlinkOptions.PRECOMBINE_FIELD.key() -> "order_id"
      FlinkOptions.OPERATION.key() -> WriteOperationType.UPSERT.value(),

      FlinkOptions.HIVE_SYNC_ENABLED.key() -> "true",
      FlinkOptions.HIVE_SYNC_DB.key() -> "hudi_db",
      FlinkOptions.HIVE_SYNC_TABLE.key() -> "cdc_order",
      FlinkOptions.HIVE_SYNC_MODE.key() -> "hms",
      FlinkOptions.HIVE_SYNC_METASTORE_URIS.key() -> "thrift://slave-node:9083",

      FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key() -> "true",
      FlinkOptions.COMPACTION_ASYNC_ENABLED.key() -> "false",
      //      FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key() -> "num_commits",
      //      FlinkOptions.COMPACTION_DELTA_COMMITS.key() -> "2",
      FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key() -> FlinkOptions.NUM_OR_TIME,
      FlinkOptions.COMPACTION_DELTA_COMMITS.key() -> "2",
      FlinkOptions.COMPACTION_DELTA_SECONDS.key() -> "300",

      HoodieCleanConfig.ASYNC_CLEAN.key() -> "true",
      HoodieCleanConfig.CLEAN_MAX_COMMITS.key() -> "1",
      FlinkOptions.CLEAN_POLICY.key() -> "KEEP_LATEST_FILE_VERSIONS",
      FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS.key() -> "12",

      //      HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key() -> "false",
      //      HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_PORT_NUM.key() -> "21230",
      //      FileSystemViewStorageConfig.VIEW_TYPE.key() -> "MEMORY",
      //      FileSystemViewStorageConfig.REMOTE_PORT_NUM.key() -> "21231"

      HoodieIndexConfig.INDEX_TYPE.key() -> BUCKET.name,
      HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key() -> BucketIndexEngineType.SIMPLE.name(),
      HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key() -> "1",
      HoodieLayoutConfig.LAYOUT_TYPE.key() -> HoodieStorageLayout.LayoutType.BUCKET.name,
      HoodieLayoutConfig.LAYOUT_PARTITIONER_CLASS_NAME.key() -> HoodieLayoutConfig.SIMPLE_BUCKET_LAYOUT_PARTITIONER_CLASS_NAME,

      HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.key() -> "3",
      HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key() -> "optimistic_concurrency_control",
      HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() -> "LAZY",
      HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key() -> classOf[HiveMetastoreBasedLockProvider].getName,
      HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key() -> classOf[BucketIndexConcurrentFileWritesConflictResolutionStrategy].getName,
      LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY -> "hudi_db",
      LockConfiguration.HIVE_TABLE_NAME_PROP_KEY -> "cdc_order",
      LockConfiguration.HIVE_METASTORE_URI_PROP_KEY -> "thrift://slave-node:9083",
    ).asJava
    val builder = HoodiePipeline.builder("cdc_order_hudi")
      .schema(schema)
      .partition("order_time")
      .options(options)
    builder.sink(src, false)

    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
