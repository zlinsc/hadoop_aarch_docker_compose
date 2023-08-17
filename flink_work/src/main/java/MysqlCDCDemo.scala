import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object MysqlCDCDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(30000)

    val conf = ConfigFactory.load("application.conf")

    Seq(1, 2).map(_ => {
      val mysqlSource = MySqlSource.builder[String]()
        .serverId(conf.getString("mysql.serverId"))
        .hostname(conf.getString("mysql.hostname"))
        .port(conf.getInt("mysql.port"))
        .databaseList(conf.getString("mysql.database"))
        .tableList(conf.getString("mysql.table"))
        .username(conf.getString("mysql.username"))
        .password(conf.getString("mysql.password"))
        .startupOptions(StartupOptions.initial())
        .deserializer(new JsonDebeziumDeserializationSchema())
        //      .includeSchemaChanges(true)
        //      .splitSize(5)
        .serverTimeZone("Asia/Shanghai")
        .build()
      val src = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[String](), "Mysql CDC Source").asInstanceOf[DataStream[String]]
      src
    }).reduce(_.union(_)).print()

    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
