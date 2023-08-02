import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.JavaConverters._

object MysqlCDCDemo {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load("application_online.conf")
    val ips = conf.getConfigList("mysql.ip_port").asScala
    val c = ips.head
    //    ips.foreach(c => {
    val mysqlSource = MySqlSource.builder[String]()
      .serverId(conf.getString("mysql.serverId"))
      .hostname(c.getString("hostname"))
      .port(c.getInt("port"))
      .databaseList(conf.getString("mysql.database"))
      .tableList(conf.getString("mysql.table"))
      .username(conf.getString("mysql.username"))
      .password(conf.getString("mysql.password"))
      .startupOptions(StartupOptions.initial())
      .deserializer(new JsonDebeziumDeserializationSchema())
      //      .includeSchemaChanges(true)
      //      .splitSize(5)
      .build()
    //    })

    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(30000)
    env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[String](), "Mysql CDC Source")
      .print()
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
