import java.util.Properties
import java.util.concurrent.{BlockingQueue, TimeUnit}

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object TwitterProducer {
  val consumerKey = "3Yfh78XTUiOhGm3lDOug"
  val consumerSecret = "Exyna93nwEyk15vPls0b2HZmZSQXtaisCMwkhslNPM0"
  val token = "844827488-sUfq4wzNIfzW8DSjcvsrN9s31zzlIAKPsa3WP7hX"
  val secret = "gIN7FsW6zYeIwwJLJsdtlK2m0V2wdXY6UbEwiuQj6VrSm"
  val logger = LoggerFactory.getLogger("TwitterProducer")

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    logger.info("Setting Up ...")
    import java.util.concurrent.LinkedBlockingQueue
    val msgQueue = new LinkedBlockingQueue[String](1000)

    val client: Client = createTwitterClient(msgQueue)
    client.connect()
    val kafkaProducer = createKafkaProducerClient()

    logger.info("Setup completed")
    logger.info("Query messages")
    while (!client.isDone) {
      try {
        val msg = msgQueue.poll(5, TimeUnit.SECONDS)
        if (msg != null) {
          kafkaProducer.send(new ProducerRecord[String, String]("twitter", null, msg), new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception != null) {
                logger.error(exception.getMessage)
              }
            }
          })
        }
      } catch {
        case e: InterruptedException => e.printStackTrace()
        case _ => logger.error("Error")
      } finally {
        client.stop()
      }
    }
  }

  def createTwitterClient(msgQueue: BlockingQueue[String]): Client = {

    import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
    import com.twitter.hbc.core.{Constants, HttpHosts}
    import com.twitter.hbc.httpclient.auth.OAuth1
    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) *//** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint()
    // Optional: set up some followings and track terms

    val terms = Lists.newArrayList("example")
    hosebirdEndpoint.trackTerms(terms)

    // These secrets should be read from a config file
    val hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret)

    val builder = new ClientBuilder()
      .name("Hosebird-Client-01")
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))

    builder.build()
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
