import com.typesafe.config.ConfigFactory
import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object PostgresCDCDemo {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load("application.conf")
//    val postgresIncrementalSource: JdbcIncrementalSource[String] = PostgresIncrementalSource.builder[String]
    val postgresIncrementalSource = PostgreSQLSource.builder[String]
      .hostname(conf.getString("psql.hostname"))
      .port(conf.getInt("psql.port"))
      .database(conf.getString("psql.database"))
      .schemaList(conf.getString("psql.schemaList"))
      .tableList(conf.getString("psql.tableList"))
      .username(conf.getString("psql.username"))
      .password(conf.getString("psql.password"))
      .slotName("flink_work")
      .decodingPluginName("pgoutput")
      .deserializer(new JsonDebeziumDeserializationSchema())
//      .includeSchemaChanges(true)
//      .splitSize(5)
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(30000)
//    env.fromSource(postgresIncrementalSource, WatermarkStrategy.noWatermarks[String](), "Postgres CDC Source")
//      .setParallelism(2).print()
    env.addSource(postgresIncrementalSource).print()
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
