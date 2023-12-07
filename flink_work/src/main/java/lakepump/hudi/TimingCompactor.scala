package lakepump.hudi

import org.apache.hudi.common.model.HoodieCleaningPolicy
import org.apache.hudi.sink.compact.HoodieFlinkCompactor.AsyncCompactionService
import org.apache.hudi.sink.compact.{FlinkCompactionConfig, HoodieFlinkCompactor}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.collection.mutable

object TimingCompactor {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val HUDI_ROOT_PATH = "hdfs://ctyunns/user/ads/hudi"
  val SET_DB_TABLES = "dbTables"
  val SET_BUCKETS = "buckets"
  val SHARDING_MAX_NUM = 8

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

    //// do compaction
    val threadPool = Executors.newFixedThreadPool(4)
    val taskNum = tblList.size * SHARDING_MAX_NUM
    while (true) {
      LOG.info("start to compact hudi table")
      val latch = new CountDownLatch(taskNum)
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
            val task = new Runnable {
              def run(): Unit = {
                try {
                  val service = new AsyncCompactionService(cfg, conf)
                  new HoodieFlinkCompactor(service).start(cfg.serviceMode)
                } catch {
                  case e: Exception =>
                    LOG.error("Error in compaction task", e)
                } finally {
                  latch.countDown()
                }
              }
            }
            threadPool.submit(task)
          }
        } else throw new Exception("%s miss splittable point".format(x))
      })
      latch.await(60, TimeUnit.MINUTES)
      Thread.sleep(cfg.minCompactionIntervalSeconds * 1000)
    }
  }
}
