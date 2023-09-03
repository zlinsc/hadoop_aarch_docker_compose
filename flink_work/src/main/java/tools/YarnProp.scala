package tools

import org.apache.flink.configuration.{ConfigOptions, Configuration}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object YarnProp {
  def getYarnAppName(env: StreamExecutionEnvironment): String = {
    var appName = ""
    try {
      val configurationField = classOf[StreamExecutionEnvironment].getDeclaredField("configuration");
      if (!configurationField.isAccessible) configurationField.setAccessible(true)
      val configuration = configurationField.get(env).asInstanceOf[Configuration]
      appName = configuration.get(ConfigOptions.key("yarn.application.name").stringType().noDefaultValue())
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    appName
  }
}
