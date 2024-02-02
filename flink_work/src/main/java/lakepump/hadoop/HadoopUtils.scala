package lakepump.hadoop

import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.client.api.YarnClient
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

object HadoopUtils {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  val yarnClient: YarnClient = YarnClient.createYarnClient()

  def appIsRunningOrNot(appName: String): Boolean = {
    val configuration = new Configuration()
    //    configuration.set(YarnConfiguration.RM_ADDRESS, "resource-manager-host:resource-manager-port")
    yarnClient.init(configuration)
    yarnClient.start()
    var isRunning = false
    val appReports = yarnClient.getApplications().asScala
    breakable {
      for (app: ApplicationReport <- appReports) {
        if (app.getName.equals(appName) && app.getFinalApplicationStatus == null) {
          isRunning = true
          break
        }
      }
    }
    yarnClient.stop()
    isRunning
  }

  def getNewestFile(path: String): Path = {
    val p = new Path(path)
    val fs = p.getFileSystem
    val subfiles = fs.listStatus(p)
    if (fs.exists(p) && subfiles != null && subfiles.nonEmpty)
      fs.listStatus(p).map(x => (x, x.getModificationTime)).maxBy(_._2)._1.getPath
    else
      null
  }

  def getFileContent(path: Path): String = {
    val fs = path.getFileSystem
    val input = fs.open(path)
    val reader = new BufferedReader(new InputStreamReader(input))
    val content = new StringBuilder
    try {
      var line: String = reader.readLine()
      while (line != null) {
        content.append(line).append("\n")
        line = reader.readLine()
      }
    } finally {
      input.close()
    }
    content.toString().trim
  }

  def overwriteFileContent(path: Path, content: String): Unit = {
    val fs = path.getFileSystem
    val stream = fs.create(path, WriteMode.OVERWRITE)
    stream.write(content.getBytes(ConfigConstants.DEFAULT_CHARSET))
    stream.close()
  }

  def getLatestValidSavepointPath(jobidPath: Path, ckpDir: String): String = {
    val fs = jobidPath.getFileSystem
    var rsltPath = ""
    if (fs.exists(jobidPath)) {
      val lastJobID = getFileContent(jobidPath).trim
      if (lastJobID.nonEmpty) {
        val lastSavePath = getNewestFile(ckpDir + lastJobID)
        if (lastSavePath != null) {
          val sav = lastSavePath.toString
          val isMatch = "chk-\\d+".r.findFirstIn(sav).isDefined
          if (isMatch) rsltPath = sav
          else LOG.warn("savepoint file is not like chk-xxx: " + sav)
        } else LOG.warn("savepoint is not found on path: " + ckpDir + lastJobID)
      } else LOG.warn("cache file is empty")
    } else LOG.warn("first time to create this app without path " + jobidPath.toString)
    rsltPath
  }

  def getLatestValidSavepointPathV2(app2jobPath: Path, jobID: String, ckpDir: String): String = {
    val fs = app2jobPath.getFileSystem
    var rsltPathStr = ""
    if (fs.exists(app2jobPath)) {
      val ckpPath = if (jobID.nonEmpty) getNewestFile(ckpDir + jobID) else {
        val lastJobIDPath = getNewestFile(app2jobPath.toString)
        if (lastJobIDPath != null) {
          val lastJobID = lastJobIDPath.getName
          getNewestFile(ckpDir + lastJobID)
        } else {
          LOG.warn("there is no valid latest savepoint found on path: " + app2jobPath.toString)
          null
        }
      }
      if (ckpPath != null && fs.exists(ckpPath)) {
        val check = "chk-\\d+".r.findFirstIn(ckpPath.getName).isDefined
        if (check) rsltPathStr = ckpPath.toString
        else LOG.warn("savepoint file is not like chk-xxx: " + ckpPath.toString)
      } else LOG.warn("savepoint path does not exists: " + ckpPath)
    } else LOG.warn("first time to create this app without path " + app2jobPath.toString)
    rsltPathStr
  }

  def savAppName2JobID(app2jobPath: Path, jobID: String): Unit = {
    val fs = app2jobPath.getFileSystem
    fs.create(new Path(app2jobPath, jobID), WriteMode.NO_OVERWRITE)
  }

  def existValidCheckpointPath(jobID: String, ckpDir: String): Boolean = {
    var flag = false
    if (jobID.nonEmpty) {
      val ckpPath = getNewestFile(ckpDir + jobID)
      val fs = ckpPath.getFileSystem
      if (fs.exists(ckpPath)) {
        val p = ckpPath.getName
        if ("chk-\\d+".r.findFirstIn(p).isDefined) flag = true
      }
    } else throw new Exception("jobid is empty")
    flag
  }

  def main(args: Array[String]) = {
    //    val str = "2020-08-18T20:50:47Z"
    //    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    //    val instant = Instant.from(formatter.parse(str))
    //    val instant = Instant.parse(str)
    //    System.out.println(instant)
    //    println(getNewestFile("hdfs://ctyunns/user/ads/tmp"))
  }
}
