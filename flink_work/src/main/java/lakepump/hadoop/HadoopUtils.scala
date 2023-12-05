package lakepump.hadoop

import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.client.api.YarnClient

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

object HadoopUtils {
  val yarnClient: YarnClient = YarnClient.createYarnClient()
  val fileSys: FileSystem = FileSystem.get(new URI("hdfs://ctyunns/user/ads/"))

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
    if (fileSys.exists(p))
      fileSys.listStatus(p).map(x => (x, x.getModificationTime)).maxBy(_._2)._1.getPath
    else
      null
  }

  def getFileContent(path: Path): String = {
    val input = fileSys.open(path)
    val reader = new BufferedReader(new InputStreamReader(input))
    val line = reader.readLine().trim
    input.close()
    line
  }

  def overwriteFileContent(path: Path, content: String): Unit = {
    val stream = fileSys.create(path, WriteMode.OVERWRITE)
    stream.write(content.getBytes(ConfigConstants.DEFAULT_CHARSET))
    stream.close()
  }

  def main(args: Array[String]) = {

    val str = "2020-08-18T20:50:47Z"

    // 定义日期时间格式
    //    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    // 使用格式化程序解析日期字符串
    //    val instant = Instant.from(formatter.parse(str))
    val instant = Instant.parse(str)

    System.out.println(instant)
  }
}
