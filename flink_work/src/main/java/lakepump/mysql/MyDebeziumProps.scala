package lakepump.mysql

import java.util.Properties

object MyDebeziumProps {
  def getProps: Properties = {
    val properties = new Properties
    properties.setProperty("converters", "dateConverters")
    properties.setProperty("dateConverters.type", "lakepump.mysql.MySqlDateTimeConverter")
//    properties.setProperty("dateConverters.format.date", "yyyy-MM-dd")
//    properties.setProperty("dateConverters.format.time", "HH:mm:ss")
//    properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss")
//    properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss")
//    properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8")
//    properties.setProperty("debezium.snapshot.locking.mode", "none") // 全局读写锁，可能会影响在线业务，跳过锁设置
//    properties.setProperty("include.schema.changes", "true")
//    properties.setProperty("bigint.unsigned.handling.mode", "long")
//    properties.setProperty("decimal.handling.mode", "double")
    properties
  }

  def getHudiProps: Properties = {
    val properties = new Properties
    properties.setProperty("converters", "dateConverters")
    properties.setProperty("dateConverters.type", "lakepump.mysql.MySqlHudiTimeConverter")
    properties
  }
}
