package lakepump.cdc

import com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus
import com.ververica.cdc.connectors.mysql.source.assigners.state._
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset
import com.ververica.cdc.connectors.mysql.source.split._
import io.debezium.relational.TableId
import io.debezium.relational.history.TableChanges
import org.apache.flink.configuration.CheckpointingOptions
import org.apache.flink.core.fs.{EntropyInjector, Path}
import org.apache.flink.core.io.SimpleVersionedSerialization
import org.apache.flink.core.memory.{DataInputDeserializer, DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata
import org.apache.flink.runtime.checkpoint.{Checkpoints, OperatorState, StateObjectCollection}
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils
import org.apache.flink.runtime.state.OperatorStateHandle.StateMetaInfo
import org.apache.flink.runtime.state._
import org.apache.flink.runtime.state.filesystem.{AbstractFsCheckpointStorageAccess, FsCheckpointMetadataOutputStream, FsCheckpointStorageLocation}
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot
import org.apache.flink.state.api.OperatorIdentifier
import org.apache.flink.state.api.runtime.SavepointLoader
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.MathUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream, IOException}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

object TransMetaState {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val SRC_UID = OperatorIdentifier.forUid("src")
  val SOURCE_READER_STATE = "SourceReaderState"
  val ADD_VERSION = 2000

  /** read PendingSplitsState from CoordinatorState of operID */
  def readPendingSplitsState(metadata: CheckpointMetadata, operID: String): PendingSplitsState = {
    val operState = readOperatorState(metadata, operID)
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
  /*def modifyCheckpointOperatorState(env: StreamExecutionEnvironment, oldPath: String) = {
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
    val idx = oldPath.lastIndexOf("-") + 1
    val newPath = oldPath.substring(0, idx) + (oldPath.substring(idx).toLong + ADD_VERSION).toString
    //      val writer = SavepointWriter.fromExistingSavepoint(env, oldPath, new EmbeddedRocksDBStateBackend(true))
    //    val writer = SavepointWriter.newSavepoint(env, new EmbeddedRocksDBStateBackend(true), 2)
    //    writer.withOperator(identifier, trans).write(newPath)
  }*/

  /** read operator state of operID in path */
  private def readOperatorState(metadata: CheckpointMetadata, operID: String): OperatorState = {
    val operStates = metadata.getOperatorStates.asScala
    val operState = operStates.filter(x => x.getOperatorID.toString == operID).head
    operState
  }

  /** transfer coordinator state and save to metadata */
  def transformSrcState(oldPath: String, gtidsNew: String): Unit = {

    // new metadata path
    val idx = oldPath.lastIndexOf("-") + 1
    val chkSubPath = AbstractFsCheckpointStorageAccess.CHECKPOINT_DIR_PREFIX +
      (oldPath.substring(idx).toLong + ADD_VERSION).toString
    val rootPath = oldPath.substring(0, oldPath.lastIndexOf("/") + 1)
    val checkpointDir = new Path(rootPath + chkSubPath)
    val fileSystem = checkpointDir.getFileSystem

    // read operator state from old path
    val operID = SRC_UID.getOperatorId
    val operIDStr = operID.toHexString
    val metadata = SavepointLoader.loadSavepointMetadata(oldPath) // kick off
    val ckpId = metadata.getCheckpointId
    val ckpIdNew = ckpId + ADD_VERSION
    val operState = readOperatorState(metadata, operIDStr)

    // clone a new one
    val newOperState = new OperatorState(operID, operState.getParallelism, operState.getMaxParallelism)

    // reflect PartitionableListState constructor
    val constructPartListState = Class.forName("org.apache.flink.runtime.state.PartitionableListState")
      .getDeclaredConstructor(classOf[RegisteredOperatorStateBackendMetaInfo[Any]])
    constructPartListState.setAccessible(true)

    // iterate each subtask state
    val splitSerializer = new MySqlSplitSerializer
    for (es <- operState.getSubtaskStates.entrySet().asScala) {
      val subtaskIndex = es.getKey
      val operSubtaskState = es.getValue
      //      LOG.info("vvvv={}:{}", subtaskIndex, operSubtaskState.toString: Any)

      // create output stream
      val sharedStateDir = new Path(rootPath, AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR)
      val taskOwnedStateDir = new Path(rootPath, AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR)
      val fileStateSizeThreshold =
        MathUtils.checkedDownCast(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD.defaultValue().getBytes)
      val writeBufferSize = fileStateSizeThreshold
      val factory = new FsCheckpointStorageLocation(fileSystem, checkpointDir, sharedStateDir, taskOwnedStateDir,
        CheckpointStorageLocationReference.getDefault, fileStateSizeThreshold, writeBufferSize)
      val localOut = factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)

      // extract managed operator state handle in subtask state
      // need to set managed operator state explicitly because it's only accessible to builder
      val claz = operSubtaskState.getClass
      val fieldManagedOperatorState = claz.getDeclaredField("managedOperatorState")
      fieldManagedOperatorState.setAccessible(true)
      val managedOperState = fieldManagedOperatorState.get(operSubtaskState)
        .asInstanceOf[StateObjectCollection[OperatorStateHandle]]
      // FOLLOW MetadataV2V3SerializerBase#deserializeOperatorStateHandle
      val managedOperStateHandle = managedOperState.iterator().next()

      /** belong to OperatorStreamStateHandle */
      // get delegateStateHandle(StreamStateHandle, may be ByteStreamStateHandle/RelativeFileStateHandle)
      // then open it as input stream
      val in = managedOperStateHandle.openInputStream
      val div = new DataInputViewStreamWrapper(in)

      /** belong to OperatorStreamStateHandle.StreamStateHandle */
      val operMetaInfoSnapshotsNew = new ArrayBuffer[StateMetaInfoSnapshot]()
      val broadMetaInfoSnapshotsNew = new ArrayBuffer[StateMetaInfoSnapshot]()
      // FOLLOW OperatorStateRestoreOperation#restore
      // read data header using proxy
      val backendSerializationProxy = new OperatorBackendSerializationProxy(ClassLoader.getSystemClassLoader)
      backendSerializationProxy.read(div)
      // read PartitionableListState and build a map
      val restoredOperMetaInfoSnapshots = backendSerializationProxy.getOperatorStateMetaInfoSnapshots
      val registeredOperStates = mutable.Map[String, PartitionableListState[Any]]()
      for (restoredSnapshot <- restoredOperMetaInfoSnapshots.asScala) {
        val restoredMetaInfo = new RegisteredOperatorStateBackendMetaInfo(restoredSnapshot)
        val listState = constructPartListState.newInstance(restoredMetaInfo).asInstanceOf[PartitionableListState[Any]]
        val stateName = listState.getStateMetaInfo.getName
        registeredOperStates.put(stateName, listState)
        if (stateName == SOURCE_READER_STATE) operMetaInfoSnapshotsNew += listState.getStateMetaInfo.snapshot()
      }
      // create new OperatorBackendSerializationProxy to write
      val dov = new DataOutputViewStreamWrapper(localOut)
      val backendSerializationProxyNew =
        new OperatorBackendSerializationProxy(operMetaInfoSnapshotsNew.asJava, broadMetaInfoSnapshotsNew.asJava)
      backendSerializationProxyNew.write(dov)

      /** belong to OperatorStreamStateHandle */
      // partOffsets := Map[String, StateMetaInfo]
      //      val partOffsets = managedOperStateHandle.getStateNameToPartitionOffsets
      val partOffsetsNew = mutable.Map[String, StateMetaInfo]()
      //      breakable(for (nameToOffsets <- partOffsets.entrySet().asScala) {
      // only pass SOURCE_READER_STATE
      //      if (nameToOffsets.getKey != SOURCE_READER_STATE) break()

      // get PartitionableListState
      val partListState = registeredOperStates.getOrElse(SOURCE_READER_STATE, null)
      if (partListState == null) break()
      partListState.clear()
      val backendMetaInfo = partListState.getStateMetaInfo
      val mode = backendMetaInfo.getAssignmentMode
      // typeSerializer := BytePrimitiveArraySerializer
      val typeSerializer = backendMetaInfo.getPartitionStateSerializer

      // FOLLOW OperatorStateRestoreOperation#deserializeOperatorStateValues
      //        val metaInfo: OperatorStateHandle.StateMetaInfo = nameToOffsets.getValue
      //        if (null == metaInfo) break()
      //        val offsets = metaInfo.getOffsets
      //        if (null == offsets) break()
      //        for (idx <- offsets.indices) {

      /** belong to OperatorStreamStateHandle.StreamStateHandle */
      //          in.seek(offsets(idx))
      // deserialize: BytePrimitiveArraySerializer -> MySqlSplitSerializer
      val data = typeSerializer.deserialize(div).asInstanceOf[Array[Byte]]
      val mysqlSplit = SimpleVersionedSerialization.readVersionAndDeSerialize(splitSerializer, data)
      mysqlSplit match {
        case x: MySqlBinlogSplit =>

          // todo create a new binlog split
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
          // serialize: MySqlSplitSerializer -> BytePrimitiveArraySerializer
          val dataNew = SimpleVersionedSerialization.writeVersionAndSerialize(splitSerializer, mysqlSplitNew)
          // FOLLOW OperatorStateRestoreOperation#deserializeOperatorStateValues
          partListState.add(dataNew)

        case _: MySqlSnapshotSplit =>
        case _ =>
          throw new Exception("unknow state type")
      }
      //        }

      // write PartitionableListState
      val partitionOffsetsNew = partListState.write(localOut)

      // only reserve SOURCE_READER_STATE StateMetaInfo
      val metaInfoNew = new OperatorStateHandle.StateMetaInfo(partitionOffsetsNew, mode)
      partOffsetsNew(SOURCE_READER_STATE) = metaInfoNew
      //      })

      val stateHandleNew = localOut.closeAndGetHandle()
      if (stateHandleNew != null) {
        // create a new managedOperStateHandle
        val managedOperStateHandleNew = new OperatorStreamStateHandle(partOffsetsNew.asJava, stateHandleNew)

        // add updated operSubtaskState to the new OperatorState
        fieldManagedOperatorState.set(operSubtaskState, StateObjectCollection.singleton(managedOperStateHandleNew))
        newOperState.putState(subtaskIndex, operSubtaskState)
      } else throw new IOException("Stream was already unregistered.")
    }

    // todo generate new pending splits state
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

    // save checkpoint metadata
    val collect = Iterable(newOperState) // todo ++ reservedOperatorStates
    val ckpMeta = new CheckpointMetadata(ckpIdNew, collect.asJavaCollection, metadata.getMasterStates,
      metadata.getCheckpointProperties)
    val metadataDir = EntropyInjector.removeEntropyMarkerIfPresent(fileSystem, checkpointDir)
    val metadataFilePath = new Path(metadataDir, AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME)
    val ckpOut = new FsCheckpointMetadataOutputStream(fileSystem, metadataFilePath, checkpointDir)
    Checkpoints.storeCheckpointMetadata(ckpMeta, ckpOut)
    ckpOut.closeAndFinalizeCheckpoint()
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
