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
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.CheckpointingOptions
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.core.fs.{EntropyInjector, Path}
import org.apache.flink.core.io.SimpleVersionedSerialization
import org.apache.flink.core.memory.{DataInputDeserializer, DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata
import org.apache.flink.runtime.checkpoint.{Checkpoints, OperatorState, StateObjectCollection}
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils
import org.apache.flink.runtime.state.OperatorStateHandle.StateMetaInfo
import org.apache.flink.runtime.state._
import org.apache.flink.runtime.state.filesystem.{AbstractFsCheckpointStorageAccess, FsCheckpointMetadataOutputStream, FsCheckpointStorageLocation, RelativeFileStateHandle}
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot
import org.apache.flink.state.api.functions.StateBootstrapFunction
import org.apache.flink.state.api.{OperatorIdentifier, OperatorTransformation, SavepointReader, SavepointWriter}
import org.apache.flink.state.api.runtime.SavepointLoader
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.MathUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream, IOException}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TransMetaState {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val SRC_UID = OperatorIdentifier.forUid("src")
  val SOURCE_READER_STATE = "SourceReaderState"
  val ADD_VERSION = 2000

  /** read PendingSplitsState from CoordinatorState of operID */
  private def readPendingSplitsState(metadata: CheckpointMetadata, operID: String): PendingSplitsState = {
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
    //    val writer = SavepointWriter.fromExistingSavepoint(env, oldPath, new EmbeddedRocksDBStateBackend(true))
    //    val writer = SavepointWriter.newSavepoint(env, new EmbeddedRocksDBStateBackend(true), 2)
    //    writer.withOperator(identifier, trans).write(newPath)
  }

  /** read source reader state from old path */
  def readSourceReaderState(env: StreamExecutionEnvironment, oldPath: String): Unit = {
    val byteArrayType: TypeInformation[Array[Byte]] = TypeExtractor.getForClass(classOf[Array[Byte]])
    val reader = SavepointReader.read(env, oldPath, new EmbeddedRocksDBStateBackend(true))
    LOG.info("SRC_UID: {}", SRC_UID.toString)
    val input = reader.readListState(SRC_UID, SOURCE_READER_STATE, byteArrayType)
    input.map(in => {
      val state = SimpleVersionedSerialization.readVersionAndDeSerialize(new MySqlSplitSerializer, in)
      LOG.info("SourceReaderState: {}", state.toString)
      state match {
        case x: MySqlBinlogSplit =>
          val offsetObj = x.getStartingOffset
          LOG.info("--gtid {}", offsetObj.getGtidSet)
        case _: MySqlSnapshotSplit =>
          LOG.info("please restart this job again")
        case _ =>
          LOG.info("unknow state type")
      }
    })
  }

  /** read operator state of operID in path */
  private def readOperatorState(metadata: CheckpointMetadata, operID: String): OperatorState = {
    val operStates = metadata.getOperatorStates.asScala
    val operState = operStates.filter(x => x.getOperatorID.toString == operID).head
    operState
  }

  /** transfer coordinator state and save to metadata */
  def transformSrcState(oldPath: String, gtid: String, resetTbls: String): Unit = {

    // new metadata path
    val idx = oldPath.lastIndexOf("-") + 1
    val chkSubPath = AbstractFsCheckpointStorageAccess.CHECKPOINT_DIR_PREFIX +
      (oldPath.substring(idx).toLong + ADD_VERSION).toString
    val rootPath = oldPath.substring(0, oldPath.lastIndexOf("/") + 1)
    val checkpointDir = new Path(rootPath + chkSubPath)
    val fileSystem = checkpointDir.getFileSystem
    if (fileSystem.exists(checkpointDir)) fileSystem.delete(checkpointDir, true)

    // reflect PartitionableListState constructor
    val constructPartListState = Class.forName("org.apache.flink.runtime.state.PartitionableListState")
      .getDeclaredConstructor(classOf[RegisteredOperatorStateBackendMetaInfo[Any]])
    constructPartListState.setAccessible(true)

    // init MySqlSplitSerializer
    val splitSerializer = new MySqlSplitSerializer

    // init output stream factory
    val sharedStateDir = new Path(rootPath, AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR)
    val taskOwnedStateDir = new Path(rootPath, AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR)
    val fileStateSizeThreshold =
      MathUtils.checkedDownCast(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD.defaultValue().getBytes)
    val writeBufferSize = fileStateSizeThreshold
    val factory = new FsCheckpointStorageLocation(fileSystem, checkpointDir, sharedStateDir, taskOwnedStateDir,
      CheckpointStorageLocationReference.getDefault, fileStateSizeThreshold, writeBufferSize)

    // 1. read operator state from old path and clone a new one
    val operID = SRC_UID.getOperatorId
    val operIDHex = operID.toHexString
    val metadata = SavepointLoader.loadSavepointMetadata(oldPath) // kick off
    val ckpId = metadata.getCheckpointId
    val ckpIdNew = ckpId + ADD_VERSION
    val operState = readOperatorState(metadata, operIDHex)
    val newOperState = new OperatorState(operID, operState.getParallelism, operState.getMaxParallelism)

    // 2. iterate each subtask state
    for (es <- operState.getSubtaskStates.entrySet().asScala) {

      // get key & value
      val subtaskIndex = es.getKey
      val operSubtaskState = es.getValue
      //      LOG.info("vvvv={}:{}", subtaskIndex, operSubtaskState.toString: Any)

      // create output stream
      val localOut = factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)

      // extract managed operator state handle in subtask state
      // need to set managed operator state explicitly because it's only accessible to builder
      val claz = operSubtaskState.getClass
      val fieldManagedOperatorState = claz.getDeclaredField("managedOperatorState")
      fieldManagedOperatorState.setAccessible(true)
      val managedOperState = fieldManagedOperatorState.get(operSubtaskState)
        .asInstanceOf[StateObjectCollection[OperatorStateHandle]]
      // FOLLOW MetadataV2V3SerializerBase#deserializeOperatorStateHandle
      // return OperatorStreamStateHandle
      val managedOperStateHandle = managedOperState.iterator().next()

      // get delegateStateHandle(StreamStateHandle, may be ByteStreamStateHandle/RelativeFileStateHandle)
      // then open it as input stream
      managedOperStateHandle.getDelegateStateHandle match {
        case x: ByteStreamStateHandle =>
          LOG.info("StreamStateHandle:=ByteStreamStateHandle, {}", x.getData.length)
        case x: RelativeFileStateHandle =>
          LOG.info("StreamStateHandle:=RelativeFileStateHandle, {}", x.getFilePath.toString)
        case x =>
          LOG.info("StreamStateHandle:={}", x.getClass.toString)
      }
      val in = managedOperStateHandle.openInputStream
      val div = new DataInputViewStreamWrapper(in)

      // read data header using proxy
      // FOLLOW OperatorStateRestoreOperation#restore
      val backendSerializationProxy = new OperatorBackendSerializationProxy(ClassLoader.getSystemClassLoader)
      backendSerializationProxy.read(div)
      // read PartitionableListState and build a map
      val operMetaInfoSnapshotsNew = new ArrayBuffer[StateMetaInfoSnapshot]()
      val broadMetaInfoSnapshotsNew = new ArrayBuffer[StateMetaInfoSnapshot]()
      val restoredOperMetaInfoSnapshots = backendSerializationProxy.getOperatorStateMetaInfoSnapshots
      val registeredOperStates = mutable.Map[String, PartitionableListState[Any]]()
      for (restoredSnapshot <- restoredOperMetaInfoSnapshots.asScala) {
        val restoredMetaInfo = new RegisteredOperatorStateBackendMetaInfo(restoredSnapshot)
        val listState = constructPartListState.newInstance(restoredMetaInfo).asInstanceOf[PartitionableListState[Any]]
        val stateName = listState.getStateMetaInfo.getName
        if (stateName == SOURCE_READER_STATE) {
          registeredOperStates.put(stateName, listState)
          operMetaInfoSnapshotsNew += listState.getStateMetaInfo.snapshot()
        }
      }

      // 3. create new output stream and OperatorBackendSerializationProxy
      val dov = new DataOutputViewStreamWrapper(localOut)
      val backendSerializationProxyNew =
        new OperatorBackendSerializationProxy(operMetaInfoSnapshotsNew.asJava, broadMetaInfoSnapshotsNew.asJava)
      backendSerializationProxyNew.write(dov)

      // 4. take and clear PartitionableListState
      val partListState = registeredOperStates.getOrElse(SOURCE_READER_STATE, null)
      if (partListState == null) return
      partListState.clear()
      val backendMetaInfo = partListState.getStateMetaInfo

      // 5. deserialize byte str to get MysqlSplit
      val partOffsetsNew = mutable.Map[String, StateMetaInfo]()
      val mode = backendMetaInfo.getAssignmentMode
      // typeSerializer := BytePrimitiveArraySerializer
      val typeSerializer = backendMetaInfo.getPartitionStateSerializer
      // get offset array in offsetsMap
      val metaInfo = managedOperStateHandle.getStateNameToPartitionOffsets.getOrDefault(SOURCE_READER_STATE, null)
      if (metaInfo != null && metaInfo.getOffsets.nonEmpty) {
        LOG.info("offset={}", metaInfo.getOffsets.head)

        // deserialize: BytePrimitiveArraySerializer -> MySqlSplitSerializer
        val data = typeSerializer.deserialize(div).asInstanceOf[Array[Byte]]
        SimpleVersionedSerialization.readVersionAndDeSerialize(splitSerializer, data) match {
          case x: MySqlBinlogSplit =>

            // todo create a new binlog split
            val startingOffset = BinlogOffset.builder()
              .setServerId(x.getStartingOffset.getServerId)
              .setGtidSet(gtid)
              .setBinlogFilePosition("", Long.MinValue)
              .build()
            //            val finishedSplitsInfo = List[FinishedSnapshotSplitInfo]()
            val mysqlSplitNew = new MySqlBinlogSplit(
              "binlog-split",
              startingOffset,
              BinlogOffset.ofNonStopping,
              //              finishedSplitsInfo.asJava,
              x.getFinishedSnapshotSplitInfos,
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

        // 6. write PartitionableListState back to output stream (file)
        val partitionOffsetsNew = partListState.write(localOut)

        // 7. create new stateNameToPartitionOffsets in OperatorStreamStateHandle
        val metaInfoNew = new OperatorStateHandle.StateMetaInfo(partitionOffsetsNew, mode)
        partOffsetsNew(SOURCE_READER_STATE) = metaInfoNew
      }

      // 8. create new delegateStateHandle in OperatorStreamStateHandle
      val stateHandleNew = localOut.closeAndGetHandle()
      if (stateHandleNew != null) {

        // 9. create a new managedOperStateHandle and set back to OperatorState
        val managedOperStateHandleNew = new OperatorStreamStateHandle(partOffsetsNew.asJava, stateHandleNew)
        // add updated operSubtaskState to the new OperatorState
        fieldManagedOperatorState.set(operSubtaskState, StateObjectCollection.singleton(managedOperStateHandleNew))
        newOperState.putState(subtaskIndex, operSubtaskState)

      } else throw new IOException("Stream was already unregistered.")
    }

    // todo 10. generate new pending splits state
    val resetTbleSet = resetTbls.split(",").toSet
    val pendingSplitsStateOld = readPendingSplitsState(metadata, operIDHex).asInstanceOf[HybridPendingSplitsState]
    val alreadyProcessedTablesSetInState =
      pendingSplitsStateOld.getSnapshotPendingSplits.getAlreadyProcessedTables.asScala.toSet
    LOG.info("alreadyProcessedTablesSetInState={}", alreadyProcessedTablesSetInState.map(_.toString()).mkString(","))
    val alreadyProcessedTables = ArrayBuffer[TableId]()
    for (t <- alreadyProcessedTablesSetInState) {
      val x = "%s.%s".format(t.catalog(), t.table())
      // exclude reset tables
      if (!resetTbleSet.contains(x)) alreadyProcessedTables += t
    }
    LOG.info("alreadyProcessedTables={}", alreadyProcessedTables.map(_.toString()).mkString(","))
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

    // 11. serialize coordinator state
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

    // 12. save checkpoint metadata
    val collect = Iterable(newOperState) // ++ reservedOperatorStates
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
    val argParams = ParameterTool.fromArgs(args)
    LOG.info("argParams: {}", argParams.toMap.toString)

    // todo --path hdfs://master-node:50070/tmp/checkpoints/aebe97883e2c19b8abaf5c47b30cf35c/chk-2014
    //      --gtid d7a47357-6d10-11ee-a3cd-0242ac110002:1-11
    //      --reset_tbl test_db.cdc_order
    val oldPath = argParams.get("path", "")
    readSourceReaderState(env, oldPath)
    val gtid = argParams.get("gtid", "")
    val resetTbls = argParams.get("reset_tbl", "")
    if (gtid.nonEmpty || resetTbls.nonEmpty) transformSrcState(oldPath, gtid, resetTbls)

    env.fromElements(0).print()
    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
