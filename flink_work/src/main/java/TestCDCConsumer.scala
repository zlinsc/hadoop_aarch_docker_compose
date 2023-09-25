import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import tools.kafka.{KafkaUtils, MyDeserializationSchema}

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

object TestCDCConsumer {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  class RowKeySelector extends KeySelector[(String, String), String] with ResultTypeQueryable[(String, String)] {
    override def getKey(in: (String, String)): String = in._1

    override def getProducedType: TypeInformation[(String, String)] = TypeInformation.of(classOf[(String, String)])
  }

  class MyProcessFunc extends KeyedProcessFunction[String, (String, String), String] {
    private var cntCache: MapState[String, Boolean] = _
    //    private var hasPrint: MapState[String, Boolean] = _
    //    private var lastTime: Long = 0

    override def processElement(i: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, collector: Collector[String]): Unit = {
      if (cntCache.contains(i._1)) {
//        LOG.error("xxxxxxxx===:"+i._1+","+cntCache.get(i._1))
        if (!cntCache.get(i._1)) {
          cntCache.put(i._1, true)
          collector.collect(i._1)
        }
      } else {
        cntCache.put(i._1, false)
      }
    }

    //    override def processElement(i: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, collector: Collector[String]): Unit = {
    //      if (cntCache.contains(i._1)) {
    //        LOG.error("xxxxxxx=====:"+i._1)
    //        cntCache.put(i._1, true)
    ////        collector.collect(i._1)
    //      } else {
    //        LOG.error("xxxxxxx==2222:"+i._1)
    //        cntCache.put(i._1, false)
    //      }
    //      val ct = System.currentTimeMillis()
    //      if (ct > lastTime + 60000) {
    //        ctx.timerService().registerProcessingTimeTimer(ct)
    //        lastTime = ct
    //      }
    //    }

    //    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
    //      val it = cntCache.keys().iterator()
    //      var c = 0
    //      while (it.hasNext) {
    //        c += 1
    //        val v = it.next()
    //        if (cntCache.get(v) && !hasPrint.contains(v)) {
    //          out.collect(v)
    //          hasPrint.put(v, false)
    //        }
    //      }
    //      LOG.error("yyyyyy****:"+c)
    //    }

    override def open(parameters: Configuration): Unit = {
      cntCache = getRuntimeContext.getMapState(new MapStateDescriptor[String, Boolean]("cntCache", classOf[String], classOf[Boolean]))
//      hasPrint = getRuntimeContext.getMapState(new MapStateDescriptor[String, Boolean]("hasPrint", classOf[String], classOf[Boolean]))
//      lastTime = System.currentTimeMillis()
    }
  }

  def main(args: Array[String]): Unit = {
    // config
    val arr = args(0).split('.')
    val (db, table) = (arr(0), arr(1).toUpperCase)
    printf("CDC DB: %s, TABLE: %s\n", db, table)
    val conf = ConfigFactory.load("app_online.conf")
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val chCfg = env.getCheckpointConfig
    chCfg.setCheckpointStorage(conf.getString("flink.checkpointDir"))
    chCfg.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // source def
    var offset = OffsetsInitializer.committedOffsets()
    if (args.length > 1) {
      val date = args(1) // "2023-08-19_22:00:00"
      val time = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss"))
      val timestamp = time.toInstant(ZoneOffset.of("+8")).toEpochMilli
      offset = OffsetsInitializer.timestamp(timestamp)
      printf("Kafka start from date: %s, offset: %d\n", date, timestamp)
    }
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(KafkaUtils.getBrokerList)
//      .setTopics(conf.getString("kafka.topicTest"))
      .setTopics(args(2))
      .setGroupId("c_mysql_cdc_group")
      .setStartingOffsets(offset)
      .setDeserializer(new MyDeserializationSchema)
      //            .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperties(KafkaUtils.getDefaultProp(true))
      .build()

    // calc
    val src = env
      .fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String](), "Kafka Source")
      .uid("src")
    src.map(x => {
      val arr = x.split("###")
      var par = ""
      var tbl = ""
      if (arr.length == 2) {
        par = arr(0)
        tbl = arr(1)
      }
      (par, tbl)
    }).returns(TypeInformation.of(classOf[(String, String)]))
      .filter(x => x._1 == "{\"UID\":415801471820}")
//      .filter(x => x._2 == table)
//      .keyBy(new RowKeySelector)
//      .process(new MyProcessFunc)
      .print()
      .setParallelism(1)

    env.execute(getClass.getSimpleName.stripSuffix("$"))
  }
}
