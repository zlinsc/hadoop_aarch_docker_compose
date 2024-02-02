package lakepump.mysql

import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import io.debezium.data.Envelope
import lakepump.hudi.MysqlCDC2HudiV2.LOG
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind
import org.apache.flink.util.Collector
import org.apache.flink.util.Preconditions.checkNotNull
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

import java.time.ZoneId

class RecPackDeserializationSchema(sharding: Int, tableRowMap: Map[String, (Seq[String], RowType)])
  extends DebeziumDeserializationSchema[RecPack] {

  override def deserialize(sourceRecord: SourceRecord, out: Collector[RecPack]): Unit = {
    val topic = sourceRecord.topic()
    val arr = topic.split("\\.")
    if (arr.length == 1) {
      LOG.warn("without handler: " + sourceRecord.toString)
    } else if (arr.length == 3) {
      val db = arr(1)
      val table = arr(2)
      val regex = "_\\d+$".r
      val tagName = regex.replaceAllIn(db, "") + "." + table
      val dbTail = regex.findFirstIn(db).getOrElse("")

      val keyHash = sourceRecord.key().hashCode()
      //            val bytes = MysqlCDC2Hudi.jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value())
      //            val schemaAndValue = MysqlCDC2Hudi.jsonConverter.toConnectData(sourceRecord.topic(), bytes)
      val value = sourceRecord.value().asInstanceOf[Struct]
      val valueSchema = sourceRecord.valueSchema()
      val opField = value.schema.field("op")
      if (opField != null) {
        val fullName = "%s.%s".format(db, table)
        val conv = RowDataDeserializationRuntimeConverter.createConverter(checkNotNull(tableRowMap(fullName)._2),
          ZoneId.of("UTC"), new ShardDeserializationRuntimeConverterFactory(sharding + dbTail))

        val op = value.getString(opField.name)
        if (op == "c" || op == "r") {
          val after = value.getStruct(Envelope.FieldName.AFTER)
          val afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema
          val afterRowData = conv.convert(after, afterSchema).asInstanceOf[GenericRowData]
          afterRowData.setRowKind(RowKind.INSERT)
          out.collect(RecPack(tagName, keyHash, afterRowData))
        } else if (op == "d") {
          val before = value.getStruct(Envelope.FieldName.BEFORE)
          val beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema
          val beforeRowData = conv.convert(before, beforeSchema).asInstanceOf[GenericRowData]
          beforeRowData.setRowKind(RowKind.DELETE)
          out.collect(RecPack(tagName, keyHash, beforeRowData))
        } else if (op == "u") {
          val before = value.getStruct(Envelope.FieldName.BEFORE)
          val beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema
          val beforeRowData = conv.convert(before, beforeSchema).asInstanceOf[GenericRowData]
          beforeRowData.setRowKind(RowKind.UPDATE_BEFORE)
          out.collect(RecPack(tagName, keyHash, beforeRowData))

          val after = value.getStruct(Envelope.FieldName.AFTER)
          val afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema
          val afterRowData = conv.convert(after, afterSchema).asInstanceOf[GenericRowData]
          afterRowData.setRowKind(RowKind.UPDATE_AFTER)
          out.collect(RecPack(tagName, keyHash, afterRowData))
        } else {
          LOG.error("op %s is not support".format(op))
        }
      } else {
        LOG.error("with out op: " + value.toString)
      }
    } else {
      LOG.warn("without handler: " + sourceRecord.toString)
    }
  }

  override def getProducedType: TypeInformation[RecPack] = TypeInformation.of(classOf[RecPack])
}
