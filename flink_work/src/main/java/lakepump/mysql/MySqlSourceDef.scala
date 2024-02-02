package lakepump.mysql

import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory
import com.ververica.cdc.debezium.DebeziumDeserializationSchema

object MySqlSourceDef {
  def newInstance(sourceConfigFactory: MySqlSourceConfigFactory,
                  deserializer: RecPackDeserializationSchema): MySqlSource[RecPack] = {
    val mysqlSourceClass: Class[_] = Class.forName("com.ververica.cdc.connectors.mysql.source.MySqlSource")
    val constructor = mysqlSourceClass.getDeclaredConstructor(
      classOf[MySqlSourceConfigFactory],
      classOf[DebeziumDeserializationSchema[RecPack]])
    constructor.setAccessible(true)
    val mysqlSource = constructor.newInstance(sourceConfigFactory, deserializer).asInstanceOf[MySqlSource[RecPack]]
    mysqlSource
  }
}
