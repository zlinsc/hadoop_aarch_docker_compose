package lakepump.cdc

import com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus
import com.ververica.cdc.connectors.mysql.source.assigners.state._
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset
import com.ververica.cdc.connectors.mysql.source.split._
import io.debezium.relational.TableId
import io.debezium.relational.history.TableChanges
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.core.fs.{EntropyInjector, Path}
import org.apache.flink.core.io.SimpleVersionedSerialization
import org.apache.flink.core.memory.{DataInputDeserializer, DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.runtime.checkpoint.{OperatorState, StateObjectCollection}
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils
import org.apache.flink.runtime.state._
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle
import org.apache.flink.state.api.functions.StateBootstrapFunction
import org.apache.flink.state.api.runtime.SavepointLoader
import org.apache.flink.state.api.{OperatorIdentifier, OperatorTransformation, SavepointReader, SavepointWriter}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TransMetaState {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val SRC_UID = OperatorIdentifier.forUid("src")
  val SOURCE_READER_STATE = "SourceReaderState"

  /** read operator state of operID in path */
  def readOperatorState(path: String, operID: String): OperatorState = {
    val metadata: CheckpointMetadata = SavepointLoader.loadSavepointMetadata(path)
    val operStates = metadata.getOperatorStates.asScala
    operStates.filter(x => x.getOperatorID.toString == operID).head
  }

  /** read PendingSplitsState from CoordinatorState of operID */
  def readPendingSplitsState(path: String, operID: String): PendingSplitsState = {
    val operState = readOperatorState(path, operID)
    val stateHandle: ByteStreamStateHandle = operState.getCoordinatorState

    // coordinatorState解析
    val bais = new ByteArrayInputStream(stateHandle.getData)
    val in = new DataInputViewStreamWrapper(bais)
    try {
      val coordinatorSerdeVersion = in.readInt()
      val enumSerializerVersion = in.readInt()
      val serializedEnumChkptSize = in.readInt()
      val bytes = new Array[Byte](serializedEnumChkptSize)
      in.readFully(bytes)

      val in2 = new DataInputDeserializer(bytes)
      val splitVersion = in2.readInt()
      val stateFlag = in2.readInt()
      LOG.info("int=" + enumSerializerVersion + "," + splitVersion + "," + stateFlag) // 5,4,3

      // deserialize
      val serializer = new PendingSplitsStateSerializer(new MySqlSplitSerializer)
      val pendingSplitsState = serializer.deserialize(enumSerializerVersion, bytes) match {
        case x: HybridPendingSplitsState =>
          LOG.info("HybridPendingSplitsState: {}", x)
          //          LOG.info("[RRR] HybridPendingSplitsState:SplitFinishedOffsets: {}", x.getSnapshotPendingSplits.getSplitFinishedOffsets)
          x
        case x: SnapshotPendingSplitsState =>
          LOG.info("SnapshotPendingSplitsState: {}", x)
          x
        case x: BinlogPendingSplitsState =>
          LOG.info("BinlogPendingSplitsState: {}", x)
          x
      }
      pendingSplitsState
    } catch {
      case e: Throwable =>
        LOG.error("fail to deserialize PendingSplitsState, ", e)
        null
    } finally {
      in.close()
      bais.close()
    }
  }

  /** modify checkpoint operator state (_metadata/uuid) */
  def modifyCheckpointOperatorState(env: StreamExecutionEnvironment, oldPath: String) = {
    /* read reader state from old path */
    val byteArrayType: TypeInformation[Array[Byte]] = TypeExtractor.getForClass(classOf[Array[Byte]])
    val reader = SavepointReader.read(env, oldPath, new EmbeddedRocksDBStateBackend(true))
    val input = reader.readListState(SRC_UID, SOURCE_READER_STATE, byteArrayType)
    input.map(in => {
      val state = SimpleVersionedSerialization.readVersionAndDeSerialize(new MySqlSplitSerializer, in)
      LOG.info("SourceReaderState: {}", state.toString)
      state match {
        case x: MySqlBinlogSplit =>
          val offsetObj = x.getStartingOffset
          // printf("Filename=%s; Position=%d\n", offsetObj.getFilename, offsetObj.getPosition)
          printf("[RECOVER_CDC] startup=gtid/%s\n", offsetObj.getGtidSet)
        case _: MySqlSnapshotSplit =>
          printf("please restart this job again\n")
        case _ =>
          throw new Exception("unknow state type")
      }
    })

    /* update reader state */
    val trans = OperatorTransformation.bootstrapWith(input).transform(new StateBootstrapFunction[Array[Byte]] {
      var state: ListState[Array[Byte]] = _

      override def initializeState(context: FunctionInitializationContext): Unit = {
        state = context.getOperatorStateStore.getListState(
          new ListStateDescriptor[Array[Byte]](SOURCE_READER_STATE, byteArrayType))
      }

      override def processElement(in: Array[Byte], context: StateBootstrapFunction.Context): Unit = {
        SimpleVersionedSerialization.readVersionAndDeSerialize(new MySqlSplitSerializer, in) match {
          case x: MySqlBinlogSplit =>
            // todo: modify if we need to update gtid
            val xs = SimpleVersionedSerialization.writeVersionAndSerialize(new MySqlSplitSerializer, x)
            state.add(xs)
          case _: MySqlSnapshotSplit =>
          // just pass
          case _ =>
            throw new Exception("unknow state type")
        }
      }

      override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {}
    })

    /* write to new path */
    val diffVersion = 2000
    val idx = oldPath.lastIndexOf("-") + 1
    val newPath = oldPath.substring(0, idx) + (oldPath.substring(idx).toLong + diffVersion).toString
    //      val writer = SavepointWriter.fromExistingSavepoint(env, oldPath, new EmbeddedRocksDBStateBackend(true))
    //    val writer = SavepointWriter.newSavepoint(env, new EmbeddedRocksDBStateBackend(true), 2)
    //    writer.withOperator(identifier, trans).write(newPath)
  }

  /** transfer coordinator state and save to metadata */
  def transformSrcState(oldPath: String, gtidsNew: String): Unit = {

    /** read operator state from old path */

    // get operator info
    val operID = SRC_UID.getOperatorId
    val operIDStr = operID.toHexString
    val operState = readOperatorState(oldPath, operIDStr) // kick off

    // clone a new one
    val newOperState: OperatorState = new OperatorState(operID, operState.getParallelism, operState.getMaxParallelism)

    /** subtask state */

    // PartitionableListState reflection
    val partitionableListStateClass = Class.forName("org.apache.flink.runtime.state.PartitionableListState")
    val constructor = partitionableListStateClass.getDeclaredConstructor(
      classOf[RegisteredOperatorStateBackendMetaInfo[Any]])
    constructor.setAccessible(true)

    // iterate each subtask state
    for (x <- operState.getSubtaskStates.entrySet().asScala) {
      //      LOG.info("xxxx={}:{}", x.getKey, x.getValue.toString: Any) // 0:SubtaskState
      val operSubtaskState = x.getValue
      val claz = operSubtaskState.getClass
      val field = claz.getDeclaredField("managedOperatorState")
      field.setAccessible(true)
      val managedOperState = field.get(x).asInstanceOf[StateObjectCollection[OperatorStateHandle]]

      // MetadataV2V3SerializerBase#deserializeOperatorStateHandle
      val managedOperStateHandle: OperatorStateHandle = managedOperState.iterator().next()

      // OperatorStateRestoreOperation#restore
      val inputStream = managedOperStateHandle.openInputStream
      val userClassloader = ClassLoader.getSystemClassLoader
      val backendSerializationProxy = new OperatorBackendSerializationProxy(userClassloader)
      backendSerializationProxy.read(new DataInputViewStreamWrapper(inputStream))
      val restoredOperMetaInfoSnapshots = backendSerializationProxy.getOperatorStateMetaInfoSnapshots
      val registeredOperStates = mutable.Map[String, PartitionableListState[Any]]()
      for (restoredSnapshot <- restoredOperMetaInfoSnapshots.asScala) {
        val restoredMetaInfo = new RegisteredOperatorStateBackendMetaInfo(restoredSnapshot)
        val listState = constructor.newInstance(restoredMetaInfo).asInstanceOf[PartitionableListState[Any]]
        registeredOperStates.put(listState.getStateMetaInfo.getName, listState)
      }

      // iterate
      for (nameToOffsets <- managedOperStateHandle.getStateNameToPartitionOffsets.entrySet().asScala) {
        if (nameToOffsets.getKey == SOURCE_READER_STATE) {
          val stateListForName = registeredOperStates.getOrElse(SOURCE_READER_STATE, null)
          if (stateListForName != null) {
            val stateListForNameNew = stateListForName.deepCopy()
            stateListForNameNew.clear()
            val metaInfo: OperatorStateHandle.StateMetaInfo = nameToOffsets.getValue

            // OperatorStateRestoreOperation#deserializeOperatorStateValues
            if (null != metaInfo) {
              val offsets = metaInfo.getOffsets
              if (null != offsets) {
                val div = new DataInputViewStreamWrapper(inputStream)
                val typeSerializer = stateListForName.getStateMetaInfo.getPartitionStateSerializer
                for (offset <- offsets) {
                  inputStream.seek(offset)

                  // SimpleVersionedSerialization#readVersionAndDeSerialize
                  val data: Array[Byte] = typeSerializer.deserialize(div).asInstanceOf[Array[Byte]]
                  val splitSerializer = new MySqlSplitSerializer
                  val mysqlSplit = SimpleVersionedSerialization.readVersionAndDeSerialize(splitSerializer, data)
                  mysqlSplit match {
                    case x: MySqlBinlogSplit =>
                      // create a new split
                      val startingOffset = BinlogOffset.builder()
                        .setServerId(x.getStartingOffset.getServerId)
                        .setGtidSet(gtidsNew)
                        .build()
                      val finishedSplitsInfo = List[FinishedSnapshotSplitInfo]()
                      val mysqlSplitNew = new MySqlBinlogSplit(
                        "binlog-split",
                        startingOffset,
                        BinlogOffset.ofNonStopping,
                        finishedSplitsInfo.asJava,
                        x.getTableSchemas,
                        x.getTotalFinishedSplitSize,
                        false)
                      val dataNew = SimpleVersionedSerialization.writeVersionAndSerialize(splitSerializer, mysqlSplitNew)
                      //                      typeSerializer.asInstanceOf[BytePrimitiveArraySerializer].serialize(dataNew, dov)
                      stateListForNameNew.add(dataNew)

                    case _: MySqlSnapshotSplit =>
                    case _ =>
                      throw new Exception("unknow state type")
                  }
                }
                //
                val outputStream = new ByteArrayOutputStream()
                val dov = new DataOutputViewStreamWrapper(outputStream)

              }
            }
          }
        }
      }
    }

    /** create a new coordinator state */

    // generate new pending splits state
    val tbls = "test_db.cdc_order"
    val alreadyProcessedTables = ArrayBuffer[TableId]()
    for (t <- tbls.split(",")) {
      val arr = t.split("\\.")
      alreadyProcessedTables += new TableId(arr(0), null, arr(1))
    }
    val remainingTables = Seq[TableId]().asJava
    val remainingSplits = Seq[MySqlSchemalessSnapshotSplit]().asJava
    val assignedSplits = Map[String, MySqlSchemalessSnapshotSplit]().asJava
    val splitFinishedOffsets = Map[String, BinlogOffset]().asJava
    val tableSchemas = Map[TableId, TableChanges.TableChange]().asJava
    val assignerStatus = AssignerStatus.INITIAL_ASSIGNING
    val isTableIdCaseSensitive = true
    val state = new SnapshotPendingSplitsState(
      alreadyProcessedTables.asJava,
      remainingSplits,
      assignedSplits,
      tableSchemas,
      splitFinishedOffsets,
      assignerStatus,
      remainingTables,
      isTableIdCaseSensitive,
      true,
      new ChunkSplitterState(null, null, null))
    val isBinlogSplitAssigned = true
    val pendingSplitsState = new HybridPendingSplitsState(state, isBinlogSplitAssigned)

    // serialize coordinator state
    val baos = new ByteArrayOutputStream()
    val out = new DataOutputStream(new DataOutputViewStreamWrapper(baos))
    try {
      val serializer = new PendingSplitsStateSerializer(new MySqlSplitSerializer)
      out.writeInt(SourceCoordinatorSerdeUtils.VERSION_1)
      out.writeInt(serializer.getVersion)
      val serializedEnumChkpt = serializer.serialize(pendingSplitsState)
      out.writeInt(serializedEnumChkpt.length)
      out.write(serializedEnumChkpt)
      out.flush()
      val resultByteArray = baos.toByteArray
      val stateHandle = operState.getCoordinatorState
      val newStateHandle = new ByteStreamStateHandle(stateHandle.getHandleName, resultByteArray)
      newOperState.setCoordinatorState(newStateHandle)
    } catch {
      case e: Throwable =>
        throw new Exception("fail to generate coordinator state", e)
    } finally {
      out.close()
      baos.close()
    }

    /** generate new checkpoint metadata */
    val diffVersion = 2000
    val idx = oldPath.lastIndexOf("-") + 1
    val newPath = oldPath.substring(0, idx) + (oldPath.substring(idx).toLong + diffVersion).toString

    val metadata = SavepointLoader.loadSavepointMetadata(oldPath)
    //      val reservedOperatorStates = metadata.getOperatorStates.asScala.filter(x => x.getOperatorID.toString != operIDStr)
    val newCkpId = metadata.getCheckpointId + diffVersion
    val collect = Iterable(newOperState) //++ reservedOperatorStates
    val ckpMeta = new CheckpointMetadata(newCkpId, collect.asJavaCollection, metadata.getMasterStates,
      metadata.getCheckpointProperties)

    // save new metadata to new path
    val checkpointDir = new Path(newPath)
    val fileSystem = checkpointDir.getFileSystem
    val metadataDir = EntropyInjector.removeEntropyMarkerIfPresent(fileSystem, checkpointDir)
    val metadataFilePath = new Path(metadataDir, AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME)
    //    val ckpOut = new FsCheckpointMetadataOutputStream(fileSystem, metadataFilePath, checkpointDir)
    //    Checkpoints.storeCheckpointMetadata(ckpMeta, ckpOut)
    //    ckpOut.closeAndFinalizeCheckpoint()
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    /** path like hdfs://master-node:50070/user/root/ckp/d3bd6ff35e3dea9d0d75681cd9210941/chk-6 */
    val oldPath = args(0)
    val gtids = args(1)
    transformSrcState(oldPath, gtids)

    env.fromElements(0).print()
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
