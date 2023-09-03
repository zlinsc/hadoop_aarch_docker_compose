package tools

import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.core.io.SimpleVersionedSerialization
import org.apache.flink.state.api.{OperatorIdentifier, SavepointReader}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object RWState {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    //    val path = "hdfs://master-node:50070/user/root/checkpoints/7a58042487da30bbc2b9cbcf28d5a2cb/chk-1"
    val path = "hdfs://master-node:50070/user/root/checkpoints/d3bd6ff35e3dea9d0d75681cd9210941/chk-6"
    val savepoint: SavepointReader = SavepointReader.read(env, path, new EmbeddedRocksDBStateBackend(true))
    val byteArrayType: TypeInformation[Array[Byte]] = TypeExtractor.getForClass(classOf[Array[Byte]])
    savepoint.readListState(OperatorIdentifier.forUid("src"), "SourceReaderState", byteArrayType)
      .map { in => SimpleVersionedSerialization.readVersionAndDeSerialize(new MySqlSplitSerializer, in).toString }
      .print()

    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
