import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory


object ConsumerDemoWithThread {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("ConsumerDemo")
    val latch = new CountDownLatch(1)
    val consumerThread = new ConsumerThread(latch)
    val myThread = new Thread(consumerThread)
    myThread.start()

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        logger.info("Caught shutdown hook")
        consumerThread.close()
        try {
          latch.await()
        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
        logger.info("Application has exited")
      }
    }))

    try {
      latch.await()
    } catch {
      case e: InterruptedException => logger.error(s"Application got interrupted ${e}")
      case _ => logger.error("Unknown happened")
    } finally {
      logger.error("Application is closing")
    }
  }
}

class ConsumerThread(latch: CountDownLatch) extends Runnable {

  private val groupId = "my-first-group"
  private val properties = new Properties()
  private val offsetStra = "latest"
  private val topic: String = "first_topic"
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetStra)
  private val consumer = new KafkaConsumer[String, String](properties)
  private val logger = LoggerFactory.getLogger(classOf[ConsumerThread].getName)
  // create config
  consumer.subscribe(Collections.singleton(topic))


  def close(): Unit = {
    consumer.wakeup()
  }

  override def run(): Unit = {
    try {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(100L))
        records.forEach(record => {
          logger.info(s"Key: ${record.key()}, Value: ${record.value()}, Partition: ${record.partition()}")
        })
      }
    } catch {
      case wakeupException: WakeupException => logger.info("Received shutdown signal!")
      case _ => logger.error("Something Happened")
    } finally {
      consumer.close()
      latch.countDown()
    }

  }
}