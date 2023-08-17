import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.collection.JavaConverters._

object KafkaProp {
  val LOG: Logger = LoggerFactory.getLogger(getClass)
  val conf: Config = ConfigFactory.load("app_online.conf")

  def getConsumerDefaultProp(clientID: String) = {
    val brokerList: String = conf.getConfigList("kafka.brokerList").asScala.mkString(",")

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1048576")
    prop.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000")
    prop.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "524288")

    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientID)
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, clientID)
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    prop
  }

  def getProducerDefaultProp() = {
    val brokerList: String = conf.getConfigList("kafka.brokerList").asScala.mkString(",")

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", brokerList)
    prop.setProperty("security.protocol", "SASL_PLAINTEXT")
    prop.setProperty("sasl.mechanism", "GSSAPI")
    prop.setProperty("sasl.kerberos.service.name", "kafka")
    prop.setProperty("sasl.jaas.config", KerberosProp.getSaslJaasConfig)
    prop.setProperty("transaction.timeout.ms", "900000") // 15min
    prop
  }

  object KerberosProp {
    val keytabPath: String = conf.getString("kafka.keytabPath")
    val principal: String = conf.getString("kafka.principal") // change this according to submit node ip

    def getSaslJaasConfig = {
      "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true useTicketCache=false keyTab=\"%s\" principal=\"%s\";".format(keytabPath, principal)
    }
  }
}
