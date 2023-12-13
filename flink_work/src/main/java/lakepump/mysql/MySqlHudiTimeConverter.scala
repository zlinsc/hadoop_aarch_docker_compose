package lakepump.mysql

import io.debezium.spi.converter.CustomConverter.{Converter, ConverterRegistration}
import io.debezium.spi.converter.{CustomConverter, RelationalColumn}
import org.apache.kafka.connect.data.SchemaBuilder
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.util.Properties

class MySqlHudiTimeConverter extends CustomConverter[SchemaBuilder, RelationalColumn] {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  private val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE
  private val timeFormatter: DateTimeFormatter = DateTimeFormatter.ISO_TIME
  private val datetimeFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME
  private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME
  private val timestampZoneId = ZoneId.systemDefault()

  override def configure(props: Properties): Unit = {
  }

  override def converterFor(column: RelationalColumn, registration: ConverterRegistration[SchemaBuilder]): Unit = {
    val sqlType: String = column.typeName().toUpperCase
    var schemaBuilder: SchemaBuilder = null
    var converter: Converter = null
    sqlType match {
      case "DATE" =>
        schemaBuilder = SchemaBuilder.string.optional.name("com.darcytech.debezium.date.string")
        converter = convertDate

      case "TIME" =>
        schemaBuilder = SchemaBuilder.string.optional.name("com.darcytech.debezium.time.string")
        converter = convertTime

      case "DATETIME" =>
        schemaBuilder = SchemaBuilder.string.optional.name("com.darcytech.debezium.datetime.string")
        converter = convertDateTime

      case "TIMESTAMP" =>
        schemaBuilder = SchemaBuilder.string.optional.name("com.darcytech.debezium.timestamp.string")
        converter = convertTimestamp

      case _ =>
    }
    if (schemaBuilder != null) {
      registration.register(schemaBuilder, converter)
    }
  }

  private def convertDate(input: Any): String = {
    input match {
      case date: LocalDate =>
        dateFormatter.format(date) + "Z"
      case integer: Integer =>
        val date = LocalDate.ofEpochDay(integer.toLong)
        dateFormatter.format(date) + "Z"
      case _ => null
    }
  }

  private def convertTime(input: Any): String = {
    input match {
      case duration: Duration =>
        val seconds = duration.getSeconds
        val nano = duration.getNano
        val time = LocalTime.ofSecondOfDay(seconds).withNano(nano)
        timeFormatter.format(time) + "Z"
      case _ => null
    }
  }

  private def convertDateTime(input: Any): String = {
    input match {
      case ts: Timestamp =>
        val zonedDateTime = ZonedDateTime.ofInstant(ts.toInstant, ZoneId.of("UTC"))
        val ori = timestampFormatter.format(zonedDateTime)
        ori.substring(0, ori.indexOf("["))
      case dateTime: LocalDateTime =>
        val zonedDateTime = dateTime.atZone(timestampZoneId).withZoneSameInstant(ZoneId.of("UTC"))
        val ori = datetimeFormatter.format(zonedDateTime)
        ori.substring(0, ori.indexOf("["))
      case _ =>
        null
    }
  }

  private def convertTimestamp(input: Any): String = {
    input match {
      case ts: Timestamp =>
        val localDateTime = ts.toLocalDateTime
        val zonedDateTime = localDateTime.atZone(timestampZoneId).withZoneSameInstant(ZoneId.of("UTC"))
        val ori = timestampFormatter.format(zonedDateTime)
        ori.substring(0, ori.indexOf("["))
      case zdt: ZonedDateTime =>
        val zonedDateTime = zdt.withZoneSameInstant(timestampZoneId)
        val ori = timestampFormatter.format(zonedDateTime)
        ori.substring(0, ori.indexOf("["))
      case _ =>
        null
    }
  }
}
