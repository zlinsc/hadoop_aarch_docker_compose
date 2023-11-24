package tools.hadoop

import java.time.Instant
import java.time.format.DateTimeFormatter

object YarnUtils {
  //  def getAppID(appName: String): String = {
  //    val yarnClient = YarnClient.createYarnClient()
  //    val configuration = new Configuration()
  //    //    configuration.set(YarnConfiguration.RM_ADDRESS, "resource-manager-host:resource-manager-port")
  //    yarnClient.init(configuration)
  //    yarnClient.start()
  //    val appReports = yarnClient.getApplications(util.EnumSet.of(YarnApplicationState.RUNNING)).asScala
  //    var appID = ""
  //    breakable {
  //      for (r <- appReports) {
  //        if (r.getName.equals(appName)) {
  //          appID = r.getApplicationId.toString
  //          break
  //        }
  //      }
  //    }
  //    yarnClient.stop()
  //    appID
  //  }
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
