package tools.flink

import com.alibaba.fastjson2.JSONObject
import com.ververica.cdc.connectors.mysql.schema.MySqlTypeUtils
import io.debezium.relational.Column
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.{DecimalData, StringData}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.slf4j.{Logger, LoggerFactory}

import java.math.BigDecimal
import java.time.{Instant, ZoneId}

object RowUtils {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

//  def convertValue(x: JSONObject, colName: String, colType: String): Any = {
//    LOG.info("convertValue: " + colName + "=>" + colType)
//    convertValue(x.get(colName), transMySQLColType(colType))
//  }

  def convertValue(value: Any, dataType: DataType): Any = {
    if (value == null) {
      return null
    }
    val logicalType = dataType.getLogicalType
    logicalType match {
      case _: VarCharType | _: CharType =>
        StringData.fromString(value.asInstanceOf[String])

      case _: DoubleType | _: FloatType =>
        value.asInstanceOf[BigDecimal].doubleValue()

      case _: DecimalType =>
        val decimalPrecision = LogicalTypeChecks.getPrecision(logicalType)
        val decimalScale = LogicalTypeChecks.getScale(logicalType)
        DecimalData.fromBigDecimal(value.asInstanceOf[BigDecimal], decimalPrecision, decimalScale)

      case _: TinyIntType | _: SmallIntType | _: IntType =>
        value.asInstanceOf[Int]

      case _: BigIntType =>
        value.asInstanceOf[Long]

      case _: BooleanType =>
        value.asInstanceOf[Byte]

      case _: TimestampType | _: TimeType | _: DateType =>
        val epochMillis = value match {
          case intValue: Integer => intValue.toLong
          case longValue: Long => longValue
          case _ => throw new IllegalArgumentException("Unsupported value type")
        }
        val t = Instant.ofEpochMilli(epochMillis).atZone(ZoneId.systemDefault())
        if (logicalType.isInstanceOf[DateType]) t.toLocalDate else t.toLocalDateTime

      case _: BinaryType | _: VarBinaryType | _ =>
        value.asInstanceOf[Array[Byte]]
    }

  }

  def transMySQLColType(colType: String): DataType = {
    val t = colType.toLowerCase
    t match {
      case _ if t.contains("numeric") || t.contains("decimal") =>
        val subarr = t.substring(t.indexOf("(") + 1, t.indexOf(")")).split(",")
        subarr.length match {
          case 1 => DataTypes.DECIMAL(subarr(0).trim.toInt, 38)
          case 2 => DataTypes.DECIMAL(subarr(0).trim.toInt, subarr(1).trim.toInt)
          case _ => DataTypes.STRING()
        }
      case _ if t.contains("bigint") => DataTypes.BIGINT()
      case _ if t.contains("float") => DataTypes.FLOAT()
      case _ if t.contains("double") => DataTypes.DOUBLE()
      case _ if t.contains("boolean") => DataTypes.BOOLEAN()
      case _ if t.contains("datetime") || t.contains("timestamp") => DataTypes.TIMESTAMP()
      case _ if t.contains("time") => DataTypes.TIME()
      case _ if t.contains("date") => DataTypes.DATE()
      case _ if t.contains("varchar") || t.contains("text") || t.contains("char") || t.contains("binary") || t.contains("blob") => DataTypes.STRING()
      case _ if t.contains("tinyint") => DataTypes.TINYINT()
      case _ if t.contains("smallint") => DataTypes.SMALLINT()
      case _ if t.contains("mediumint") || t.contains("int") => DataTypes.INT()
      case _ => DataTypes.STRING()
    }
  }
}
