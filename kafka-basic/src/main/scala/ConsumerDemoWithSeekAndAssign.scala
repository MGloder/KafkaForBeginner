import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

object ConsumerDemoWithSeekAndAssign {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("ConsumerDemo")
    val properties = new Properties()
    val offsetStra = "latest"
    val topic: String = "first_topic"

    // create config
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetStra)

    val consumer = new KafkaConsumer[String, String](properties)

    val partitionToReadFrom = new TopicPartition(topic, 0)
    consumer.assign(util.Arrays.asList(partitionToReadFrom))

    val offsetToReadFrom = 478L
    consumer.seek(partitionToReadFrom, offsetToReadFrom)

    while (true) {
      val records = consumer.poll(Duration.ofMillis(100L))
      records.forEach(record => {
        logger.info(s"Key: ${record.key()}, Value: ${record.value()}, Partition: ${record.partition()}")
      })
      Thread.sleep(1000)
    }
  }
}
