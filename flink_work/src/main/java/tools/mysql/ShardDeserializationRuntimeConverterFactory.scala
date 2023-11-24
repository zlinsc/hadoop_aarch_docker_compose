package tools.mysql

import com.ververica.cdc.debezium.table.{DeserializationRuntimeConverter, DeserializationRuntimeConverterFactory}
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot, RowType}
import org.apache.kafka.connect.data.{Schema, Struct}
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZoneId
import java.util.Optional
import scala.collection.JavaConverters._

class ShardDeserializationRuntimeConverterFactory(shardingVal: String) extends DeserializationRuntimeConverterFactory {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  override def createUserDefinedConverter(logicalType: LogicalType, zoneId: ZoneId): Optional[DeserializationRuntimeConverter] = {
    logicalType.getTypeRoot match {
      case LogicalTypeRoot.ROW =>
        Optional.ofNullable(createRowConverter(logicalType.asInstanceOf[RowType], zoneId))
      case _ =>
        throw new UnsupportedOperationException("Unsupported type: " + logicalType)
    }
  }

  private def getFieldNames(rowType: RowType): Array[String] = {
    rowType.getFieldNames.toArray(Array[String]())
  }

  private def getFieldConverters(rowType: RowType, serverTimeZone: ZoneId): Array[DeserializationRuntimeConverter] = {
    rowType.getFields.asScala.map(field =>
      RowDataDeserializationRuntimeConverter.createConverter(field.getType, serverTimeZone,
        DeserializationRuntimeConverterFactory.DEFAULT)).toArray
  }

  private def createRowConverter(rowType: RowType, serverTimeZone: ZoneId): DeserializationRuntimeConverter = {
    val arity = rowType.getFieldCount
    val fieldConverters = getFieldConverters(rowType, serverTimeZone)

    new DeserializationRuntimeConverter {
      override def convert(dbzObj: Any, schema: Schema): Object = {
        val struct = dbzObj.asInstanceOf[Struct]
        val fieldsList = schema.fields()
        val fieldsListSize = fieldsList.size()
        //        LOG.info("vvvvvvv=====fieldNames:" + fieldNames.mkString(";"))
        //        LOG.info("vvvvvvvvv===schema:" + schema.fields().asScala.map(_.name()).mkString(";"))
        //        LOG.info("vvvvvvvvvvv=struct:" + struct.schema().fields().asScala.map(_.name()).mkString(";"))
        val row: GenericRowData = new GenericRowData(arity)
        for (i <- 0 until arity) {
          if (i >= fieldsListSize) {
            var convertedField: Object = null
            if (i == arity - 1) {
              // sharding column
              //              val conv = RowDataDeserializationRuntimeConverter.createConverter(
              //                DataTypes.STRING().getLogicalType, serverTimeZone, DeserializationRuntimeConverterFactory.DEFAULT)
              convertedField = convertField(fieldConverters(i), shardingVal, Schema.STRING_SCHEMA)
            } else {
              LOG.warn("column index %d is missing".format(i))
              row.setField(i, convertedField)
            }
          } else {
            val fieldName = fieldsList.get(i).name()
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

  private def convertField(fieldConverter: DeserializationRuntimeConverter, fieldValue: Object, fieldSchema: Schema): AnyRef = {
    if (fieldValue == null) null
    else fieldConverter.convert(fieldValue, fieldSchema)
  }

}
