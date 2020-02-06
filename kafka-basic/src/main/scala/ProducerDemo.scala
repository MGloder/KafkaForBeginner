import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object ProducerDemo {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    // create a producer record
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("first_topic", "Message From IDEA")
    producer.send(record)
    producer.flush()
    producer.close()

  }
}
