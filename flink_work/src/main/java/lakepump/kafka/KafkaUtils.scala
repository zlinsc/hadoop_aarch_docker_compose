package lakepump.kafka

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.admin.{AdminClient, TopicDescription}
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties
import scala.collection.JavaConverters._

object KafkaUtils {
  val conf: Config = ConfigFactory.load("app_online.conf")

  def getBrokerList: String = {
    // query broker list
    //    val adminClient = AdminClient.create(getProducerDefaultProp)
    //    val result = adminClient.describeCluster()
    //    val brokers = result.nodes.get
    val brokerList: String = conf.getStringList("kafka.brokerList").asScala.mkString(",")
    brokerList
  }

  def getConsumerDefaultProp: Properties = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", getBrokerList)
    prop.setProperty("security.protocol", "SASL_PLAINTEXT")
    prop.setProperty("sasl.mechanism", "GSSAPI")
    prop.setProperty("sasl.kerberos.service.name", "kafka")
    prop.setProperty("sasl.jaas.config", getSaslJaasConfig)
    prop.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1048576")
    prop.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000")
    prop.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "524288")
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    //    prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientID)
    //    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, clientID)
    prop
  }

  def getDefaultProp(online: Boolean): Properties = {
    val prop = new Properties()
    prop.setProperty("transaction.timeout.ms", "900000") // 15min
    if (online) {
      prop.setProperty("bootstrap.servers", getBrokerList)
      prop.setProperty("security.protocol", "SASL_PLAINTEXT")
      prop.setProperty("sasl.mechanism", "GSSAPI")
      prop.setProperty("sasl.kerberos.service.name", "kafka")
      prop.setProperty("sasl.jaas.config", getSaslJaasConfig)
      //      prop.setProperty("sasl.login.refresh.window.factor", "0.8")
    } else {
      prop.setProperty("bootstrap.servers", "172.20.0.2:9092")
    }
    prop
  }

  def getSaslJaasConfig: String = {
    val keytabPath: String = conf.getString("kafka.keytabPath")
    val principal: String = conf.getString("kafka.principal") // change this according to submit node ip
    "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"%s\" principal=\"%s\";".format(keytabPath, principal)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val adminClient = AdminClient.create(getDefaultProp(true))
    val listTopics = adminClient.listTopics()
    listTopics.names().get().forEach(println)

//    val describeResult = adminClient.describeTopics(Set("topic_ctyun_teledb_cdc_performance_prod").asJava)
//    val td: TopicDescription = describeResult.allTopicNames().get.get("topic_ctyun_teledb_cdc_performance_prod")
//    println(td)

    // query topic desc
    //    val topics: ListTopicsResult = adminClient.listTopics()
    //    val topicNames = topics.names.get
    // query group desc
    //    val groupsResult = adminClient.listConsumerGroups()
    //    val groups = groupsResult.all.get

    env.fromElements(1).print()
    env.execute()
  }

}
