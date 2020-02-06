import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import partitioner.SimpleCustomPartitioner

object ProducerDemoKeyAndCustomPartitioner {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("ProducerDemoWithKey")

    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[SimpleCustomPartitioner].getName)

    val producer = new KafkaProducer[String, String](properties)

    (1 to 100).foreach {
      index => {
        val record = new ProducerRecord[String, String]("first_topic",
          s"key_${index}",
          s"Message From IDEA with partitioner index: ${index}"
        )
        producer.send(record, (recordMetadata: RecordMetadata, e: Exception) => {
          e match {
            case e: Exception => logger.error(s"Error while producing ${e}")
            case _ => logger.info(s"Received new metadata. \n" +
              s"Topic: ${recordMetadata.topic()} \n " +
              s"Partition: ${recordMetadata.partition()} \n " +
              s"Offset: ${recordMetadata.offset()} \n " +
              s"Timestamp: ${recordMetadata.timestamp()}")
          }
        })
        producer.flush()
      }
    }
    producer.close()
  }
}
