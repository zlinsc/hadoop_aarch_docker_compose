package lakepump.cdc

import com.ververica.cdc.connectors.mysql.source.split.{MySqlBinlogSplit, MySqlSnapshotSplit, MySqlSplitSerializer}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.core.io.SimpleVersionedSerialization
import org.apache.flink.state.api.{OperatorIdentifier, SavepointReader}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object CDCStateReader {
  //  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    //    val path = "hdfs://master-node:50070/user/root/checkpoints/d3bd6ff35e3dea9d0d75681cd9210941/chk-6"
    val path = args(0)
    val savepoint: SavepointReader = SavepointReader.read(env, path, new EmbeddedRocksDBStateBackend(true))
    val byteArrayType: TypeInformation[Array[Byte]] = TypeExtractor.getForClass(classOf[Array[Byte]])
    savepoint.readListState(OperatorIdentifier.forUid("src"), "SourceReaderState", byteArrayType)
      .map(in => {
        val state = SimpleVersionedSerialization.readVersionAndDeSerialize(new MySqlSplitSerializer, in)
        printf("[RECOVER_CDC] full stat: %s\n", state.toString)
        state match {
          case x: MySqlBinlogSplit =>
            val offsetObj = x.getStartingOffset
            printf("[RECOVER_CDC] Filename=%s; Position=%d\n", offsetObj.getFilename, offsetObj.getPosition)
            printf("[RECOVER_CDC] GtidSet=%s\n", offsetObj.getGtidSet)
          case _: MySqlSnapshotSplit =>
            printf("[RECOVER_CDC] please restart this job again\n")
          case _ =>
            throw new Exception("unknow state type")
        }
      })

    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
