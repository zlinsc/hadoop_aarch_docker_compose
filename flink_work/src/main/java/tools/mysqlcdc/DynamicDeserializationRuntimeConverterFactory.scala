package tools.mysql

import com.ververica.cdc.debezium.table.{DeserializationRuntimeConverter, DeserializationRuntimeConverterFactory}
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot, RowType}
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import tools.mysqlcdc.RowDataDeserializationRuntimeConverter

import java.time.ZoneId
import java.util.Optional
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

class DynamicDeserializationRuntimeConverterFactory extends DeserializationRuntimeConverterFactory {
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
    var fieldNames = getFieldNames(rowType)
    var fieldConverters = getFieldConverters(rowType, serverTimeZone)

    new DeserializationRuntimeConverter {
      override def convert(dbzObj: Any, schema: Schema): Object = {
        val fieldsInSchema: Set[String] = schema.fields().asScala.map(field => field.name()).toSet
        val fieldNamesSet = fieldNames.toSet
        breakable {
          for (f <- fieldsInSchema) {
            if (!fieldNamesSet.contains(f)) {
              val newRowType = updateFields()
              fieldNames = getFieldNames(newRowType)
              fieldConverters = getFieldConverters(newRowType, serverTimeZone)
              break()
            }
          }
        }

        val struct: Struct = dbzObj.asInstanceOf[Struct]
        val arity: Int = fieldNames.length
        val row: GenericRowData = new GenericRowData(arity)
        for (i <- 0 until arity) {
          val fieldName: String = fieldNames(i)
          val field: Field = schema.field(fieldName)
          if (field == null) {
            row.setField(i, null)
          } else {
            val fieldValue: Object = struct.getWithoutDefault(fieldName)
            val fieldSchema: Schema = schema.field(fieldName).schema()
            val convertedField: Object = convertField(fieldConverters(i), fieldValue, fieldSchema)
            row.setField(i, convertedField)
          }
        }
        row
      }
    }
  }

  // todo
  private def updateFields(): RowType = {
    null
  }

  private def convertField(fieldConverter: DeserializationRuntimeConverter, fieldValue: Object, fieldSchema: Schema): AnyRef = {
    if (fieldValue == null) null
    else fieldConverter.convert(fieldValue, fieldSchema)
  }

}
