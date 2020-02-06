import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

object ConsumerDemo {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("ConsumerDemo")
    val groupId = "my-first-group-new"
    val properties = new Properties()
    val offsetStra = "earliest"
    val topic: String = "nyctime_all_records"

    // create config
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetStra)

    val consumer = new KafkaConsumer[String, String](properties)

    consumer.subscribe(Collections.singleton(topic))

    while (true) {
      val records = consumer.poll(Duration.ofMillis(100L))
      records.forEach(record => {
        logger.info(s"Key: ${record.key()}, Value: ${record.value()}, Partition: ${record.partition()}")
      })
      Thread.sleep(1000)
    }
  }
}
