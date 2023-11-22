package tools.mysqlcdc

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct
import com.ververica.cdc.debezium.table._
import com.ververica.cdc.debezium.utils.TemporalConversions
import io.debezium.data.VariableScaleDecimal
import io.debezium.time._
import org.apache.flink.table.data._
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{DecimalType, LogicalType, RowType}
import org.apache.kafka.connect.data.{Decimal, Schema}

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object RowDataDeserializationRuntimeConverter {

  /** Creates a runtime converter which is null safe. */
  def createConverter(`type`: LogicalType, serverTimeZone: ZoneId, userDefinedConverterFactory: DeserializationRuntimeConverterFactory) =
    wrapIntoNullableConverter(createNotNullConverter(`type`, serverTimeZone, userDefinedConverterFactory))

  /** Creates a runtime converter which assuming input object is not null. */
  def createNotNullConverter(`type`: LogicalType, serverTimeZone: ZoneId, userDefinedConverterFactory: DeserializationRuntimeConverterFactory): DeserializationRuntimeConverter = { // user defined converter has a higher resolve order
    val converter = userDefinedConverterFactory.createUserDefinedConverter(`type`, serverTimeZone)
    if (converter.isPresent) converter.get
    // if no matched user defined converter, fallback to the default converter
    `type`.getTypeRoot match {
      case NULL =>
        new DeserializationRuntimeConverter() {
          override def convert(dbzObj: Any, schema: Schema): Object = null
        }
      case BOOLEAN =>
        convertToBoolean
      case TINYINT =>
        new DeserializationRuntimeConverter() {
          override def convert(dbzObj: Any, schema: Schema): Object = java.lang.Byte.parseByte(dbzObj.toString).asInstanceOf[Object]
        }
      case SMALLINT =>
        new DeserializationRuntimeConverter() {
          override def convert(dbzObj: Any, schema: Schema): Object = java.lang.Short.parseShort(dbzObj.toString).asInstanceOf[Object]
        }
      case INTEGER | INTERVAL_YEAR_MONTH =>
        convertToInt
      case BIGINT | INTERVAL_DAY_TIME =>
        convertToLong
      case DATE =>
        convertToDate
      case TIME_WITHOUT_TIME_ZONE =>
        convertToTime
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        convertToTimestamp(serverTimeZone)
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        convertToLocalTimeZoneTimestamp(serverTimeZone)
      case FLOAT =>
        convertToFloat
      case DOUBLE =>
        convertToDouble
      case CHAR | VARCHAR =>
        convertToString
      case BINARY | VARBINARY =>
        convertToBinary
      case DECIMAL =>
        createDecimalConverter(`type`.asInstanceOf[DecimalType])
      case ROW =>
        createRowConverter(`type`.asInstanceOf[RowType], serverTimeZone, userDefinedConverterFactory)
      case ARRAY | MAP | MULTISET | RAW | _ =>
        throw new UnsupportedOperationException("Unsupported type: " + `type`)
    }
  }

  private def convertToBoolean: DeserializationRuntimeConverter = new DeserializationRuntimeConverter {
    override def convert(dbzObj: Any, schema: Schema): Object = {
      val r = dbzObj match {
        case bool: Boolean => bool
        case byte: Byte => byte == 1
        case short: Short => short == 1
        case other => java.lang.Boolean.parseBoolean(other.toString)
      }
      r.asInstanceOf[Object]
    }
  }

  private def convertToInt: DeserializationRuntimeConverter = new DeserializationRuntimeConverter() {
    override def convert(dbzObj: Any, schema: Schema): Object = {
      val r = dbzObj match {
        case i: Integer => i
        case l: Long => l.intValue
        case _ => dbzObj.toString.toInt
      }
      r.asInstanceOf[Object]
    }
  }

  private def convertToLong: DeserializationRuntimeConverter = new DeserializationRuntimeConverter {
    override def convert(dbzObj: Any, schema: Schema): Object = {
      val r = dbzObj match {
        case i: Integer => i.longValue()
        case l: Long => l
        case _ => java.lang.Long.parseLong(dbzObj.toString)
      }
      r.asInstanceOf[Object]
    }
  }

  private def convertToDouble: DeserializationRuntimeConverter = new DeserializationRuntimeConverter() {
    override def convert(dbzObj: Any, schema: Schema): Object = {
      val r = dbzObj match {
        case f: Float => f.toDouble
        case d: Double => d
        case _ => dbzObj.toString.toDouble
      }
      r.asInstanceOf[Object]
    }
  }

  private def convertToFloat: DeserializationRuntimeConverter = new DeserializationRuntimeConverter() {
    override def convert(dbzObj: Any, schema: Schema): Object = {
      val r = dbzObj match {
        case f: Float => f
        case d: Double => d.toFloat
        case _ => java.lang.Float.parseFloat(dbzObj.toString)
      }
      r.asInstanceOf[Object]
    }
  }

  private def convertToDate: DeserializationRuntimeConverter = new DeserializationRuntimeConverter {
    override def convert(dbzObj: Any, schema: Schema): Object = TemporalConversions.toLocalDate(dbzObj).toEpochDay.toInt.asInstanceOf[Object]
  }

  private def convertToTime: DeserializationRuntimeConverter = new DeserializationRuntimeConverter() {
    override def convert(dbzObj: Any, schema: Schema): Object = {
      val r = dbzObj match {
        case l: Long =>
          schema.name match {
            case MicroTime.SCHEMA_NAME => l.toInt / 1000
            case NanoTime.SCHEMA_NAME => l.toInt / 1000000
          }
        case i: Integer => i
        case _ =>
          // get number of milliseconds of the day
          TemporalConversions.toLocalTime(dbzObj).toSecondOfDay * 1000
      }
      r.asInstanceOf[Object]
    }
  }

  private def convertToTimestamp(serverTimeZone: ZoneId): DeserializationRuntimeConverter = new DeserializationRuntimeConverter() {
    override def convert(dbzObj: Any, schema: Schema): Object = {
      val r = dbzObj match {
        case longObj: Long =>
          schema.name match {
            case Timestamp.SCHEMA_NAME =>
              TimestampData.fromEpochMillis(longObj)
            case MicroTimestamp.SCHEMA_NAME =>
              val micro = longObj
              TimestampData.fromEpochMillis(micro / 1000, (micro % 1000 * 1000).toInt)
            case NanoTimestamp.SCHEMA_NAME =>
              val nano = longObj
              TimestampData.fromEpochMillis(nano / 1000000, (nano % 1000000).toInt)
          }

        case _ =>
          val localDateTime = TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone)
          TimestampData.fromLocalDateTime(localDateTime)
      }
      r.asInstanceOf[Object]
    }
  }

  private def convertToLocalTimeZoneTimestamp(serverTimeZone: ZoneId): DeserializationRuntimeConverter = {
    new DeserializationRuntimeConverter() {
      override def convert(dbzObj: Any, schema: Schema): Object = {
        val r = dbzObj match {
          case str: String =>
            val instant = Instant.parse(str)
            TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(instant, serverTimeZone))

          case _ =>
            throw new IllegalArgumentException(
              s"Unable to convert to TimestampData from unexpected value '$dbzObj' of type ${dbzObj.getClass.getName}"
            )
        }
        r.asInstanceOf[Object]
      }
    }
  }

  private def convertToString: DeserializationRuntimeConverter = new DeserializationRuntimeConverter() {
    override def convert(dbzObj: Any, schema: Schema): Object = StringData.fromString(dbzObj.toString).asInstanceOf[Object]
  }

  private def convertToBinary: DeserializationRuntimeConverter = new DeserializationRuntimeConverter() {
    override def convert(dbzObj: Any, schema: Schema): Object = {
      val r = dbzObj match {
        case bytes: Array[Byte] => bytes
        case byteBuffer: ByteBuffer =>
          val bytes = new Array[Byte](byteBuffer.remaining)
          byteBuffer.get(bytes)
          bytes
        case _ =>
          throw new UnsupportedOperationException("Unsupported BYTES value type: " + dbzObj.getClass.getSimpleName)
      }
      r.asInstanceOf[Object]
    }
  }

  private def createDecimalConverter(decimalType: DecimalType): DeserializationRuntimeConverter = {
    val precision = decimalType.getPrecision
    val scale = decimalType.getScale
    new DeserializationRuntimeConverter {
      def convert(dbzObj: Any, schema: Schema): Object = {
        val bigDecimal = dbzObj match {
          case bytes: Array[Byte] =>
            // decimal.handling.mode=precise
            Decimal.toLogical(schema, bytes)

          case str: String =>
            // decimal.handling.mode=string
            new BigDecimal(str)

          case dbl: Double =>
            // decimal.handling.mode=double
            BigDecimal.valueOf(dbl)

          case struct: Struct if VariableScaleDecimal.LOGICAL_NAME.equals(schema.name()) =>
            // variable scale decimal
            val decimal = VariableScaleDecimal.toLogical(struct)
            decimal.getDecimalValue.orElse(BigDecimal.ZERO)

          case _ =>
            // fallback to string
            new BigDecimal(dbzObj.toString)
        }
        DecimalData.fromBigDecimal(bigDecimal, precision, scale).asInstanceOf[Object]
      }
    }
  }

  private def createRowConverter(rowType: RowType, serverTimeZone: ZoneId,
                                 userDefinedConverterFactory: DeserializationRuntimeConverterFactory
                                ): DeserializationRuntimeConverter = {
    val fieldConverters: Array[DeserializationRuntimeConverter] = rowType.getFields.map(_.getType)
      .map(logicType => createConverter(logicType, serverTimeZone, userDefinedConverterFactory))
      .toArray
    val fieldNames: Array[String] = rowType.getFieldNames.toArray(Array.empty)
    new DeserializationRuntimeConverter {
      override def convert(dbzObj: Any, schema: Schema): Object = {
        val struct = dbzObj.asInstanceOf[Struct]
        val arity = fieldNames.length
        val row = new GenericRowData(arity)
        for (i <- 0 until arity) {
          val fieldName = fieldNames(i)
          val field = schema.field(fieldName)
          if (field == null) {
            row.setField(i, null)
          } else {
            val fieldValue = struct.getWithoutDefault(fieldName)
            val fieldSchema = schema.field(fieldName).schema()
            val convertedField = convertField(fieldConverters(i), fieldValue, fieldSchema)
            row.setField(i, convertedField)
          }
        }
        row
      }
    }
  }

  private def convertField(fieldConverter: DeserializationRuntimeConverter, fieldValue: Any, fieldSchema: Schema): AnyRef = {
    if (fieldValue == null) null
    else fieldConverter.convert(fieldValue, fieldSchema)
  }

  private def wrapIntoNullableConverter(converter: DeserializationRuntimeConverter): DeserializationRuntimeConverter = {
    new DeserializationRuntimeConverter() {
      override def convert(dbzObj: Any, schema: Schema): Object = {
        if (dbzObj == null) null
        else converter.convert(dbzObj, schema)
      }
    }
  }
}
