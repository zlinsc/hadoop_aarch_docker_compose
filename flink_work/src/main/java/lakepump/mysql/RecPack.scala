package lakepump.mysql

import org.apache.flink.table.data.RowData

case class RecPack(tag: String, pk: Int, row: RowData)
