import java.util.Properties
import java.util.concurrent.{BlockingQueue, TimeUnit}

import nyc.{NYCClientBuilder, NYClient}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object NYTimeProducer {
  val logger = LoggerFactory.getLogger("NYTimeProducer")

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    logger.info("Setting Up ...")
    import java.util.concurrent.LinkedBlockingQueue
    val msgQueue = new LinkedBlockingQueue[String](1000)

    val client: NYClient = createNYClient(msgQueue)
    logger.info("Setup completed")
    logger.info("Query messages")
    client.connect()

    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    //    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10")
    // default 5 to get idempotent producer
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(Integer.valueOf(32 * 1024)))
    //    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "MAX_INT")

    val producer = new KafkaProducer[String, String](properties)

    while (!client.isDone) {
      try {
        val msg = msgQueue.poll(5, TimeUnit.SECONDS)

        if (msg != null) {
          val record = new ProducerRecord[String, String]("nyctime_all_records", null, msg)
          producer.send(record)
          producer.flush()
        }
      } catch {
        case exception: Exception => logger.error(s"$exception")
        case _ => logger.error("something error occurred")
      }
    }
    producer.close()
  }

  def createNYClient(msgQueue: BlockingQueue[String]): NYClient = {
    val builder = new NYCClientBuilder
    return builder
      .addApi("gRjmsLP2vwWQ4G9yNhgNXqvyoU1ll0bL")
      .addQueue(msgQueue)
      .build()
  }

  def createKafkaProducerClient(): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//    properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[SimpleCustomPartitioner].getName)

    val producer = new KafkaProducer[String, String](properties)
    producer
  }
}
