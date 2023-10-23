package tools.hadoop

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
}
