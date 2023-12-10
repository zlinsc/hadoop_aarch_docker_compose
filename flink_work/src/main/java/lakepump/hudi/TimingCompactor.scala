package lakepump.hudi

import org.apache.hudi.common.model.HoodieCleaningPolicy
import org.apache.hudi.sink.compact.HoodieFlinkCompactor.AsyncCompactionService
import org.apache.hudi.sink.compact.{FlinkCompactionConfig, HoodieFlinkCompactor}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent._
import scala.collection.mutable

object TimingCompactor {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val HUDI_ROOT_PATH = "hdfs://ctyunns/user/ads/hudi"
  val SET_DB_TABLES = "dbTables"
  val SET_BUCKETS = "buckets"
  val SHARDING_MAX_NUM = 8

  class MyBlockingQueue[T](size: Int) extends LinkedBlockingQueue[T](size) {
    override def offer(t: T): Boolean = {
      try {
        put(t)
        true
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          false
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //// extract argsMap
    println("args: " + args.mkString(";"))
    val argsMap = mutable.Map[String, String]()
    args.foreach(x => {
      val arr = x.split("=")
      if (arr.size == 2) argsMap += (arr(0) -> arr(1))
      else throw new Exception("args error: " + args.mkString(";"))
    })

    val tables = argsMap(SET_DB_TABLES) // db.table
    val tblList = tables.split(",")

    val buckets = argsMap(SET_BUCKETS).split(",")
    if (buckets.length != tblList.length) throw new Exception("buckets list size is not equal to tables list")
    val bucketMap = mutable.Map[String, Int]()
    for (i <- 0 until tblList.size) bucketMap += (tblList(i) -> buckets(i).toInt)

    //// compaction config
    val cfg = new FlinkCompactionConfig()
    cfg.cleanPolicy = HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name()
    cfg.cleanRetainCommits = 10080 // 1 week = 7*24*60 minutes
    cfg.minCompactionIntervalSeconds = 600 // 10 minutes = 10*60 seconds
    cfg.serviceMode = false
    cfg.archiveMinCommits = cfg.cleanRetainCommits + 10
    cfg.archiveMaxCommits = cfg.archiveMinCommits + 10

    //// create thread pool
    val corePoolSize = 4
    val maximumPoolSize = 4
    val keepAliveTime = 0
    val unit = TimeUnit.SECONDS
    val workQueue: BlockingQueue[Runnable] = new MyBlockingQueue[Runnable](16)
    val threadFactory = Executors.defaultThreadFactory()
    val threadPool = new ThreadPoolExecutor(
      corePoolSize,
      maximumPoolSize,
      keepAliveTime,
      unit,
      workQueue,
      threadFactory
    )

    //// do compaction
    while (true) {
      LOG.info("start to compact hudi table")
      tblList.foreach(x => {
        cfg.compactionTasks = Math.min(bucketMap(x), 16)
        val arr = x.split("\\.")
        if (arr.size == 2) {
          val db = "hudi_" + arr(0)
          for (i <- 0 until SHARDING_MAX_NUM) {
            val table = arr(1) + "_" + i
            cfg.path = "%s/%s/%s/".format(HUDI_ROOT_PATH, db, table)
            LOG.info("compact hudi table path: " + cfg.path)
            val conf = FlinkCompactionConfig.toFlinkConfig(cfg)
            val runnableTask: Runnable = () => {
              try {
                val service = new AsyncCompactionService(cfg, conf)
                new HoodieFlinkCompactor(service).start(cfg.serviceMode)
              } catch {
                case e: Exception =>
                  LOG.error("Error in compaction task", e)
              }
            }
            threadPool.submit(runnableTask)
          }
        } else throw new Exception("%s miss splittable point".format(x))
      })
      Thread.sleep(cfg.minCompactionIntervalSeconds * 1000)
    }
  }
}
