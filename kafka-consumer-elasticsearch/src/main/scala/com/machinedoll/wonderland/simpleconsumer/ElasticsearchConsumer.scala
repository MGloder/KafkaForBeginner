import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

object ElasticsearchConsumer {
  val logger = LoggerFactory.getLogger("ElasticsearchConsumer")

  def main(args: Array[String]): Unit = {

    val client = createClient

    val consumer: KafkaConsumer[String, String] = createKafkaConsumer("nyctime_all_records")

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100L))
      logger.info(s"Received: ${records.count()}")
      records.forEach(record => {
        val recordId = record.topic() + "_" + record.partition() + "_" + record.offset()
        val indexRequest = new IndexRequest(
          "nyc",
          "url",
          recordId
        ).source(record.value(), XContentType.JSON)
        val indexResponse = client.index(indexRequest, RequestOptions.DEFAULT)
        val id = indexResponse.getId
        logger.info(id)
      })
      Thread.sleep(10)
      logger.info("Committing offsets...")
      consumer.commitSync()
      logger.info("Offset committed ")
      Thread.sleep(1000)

    }
    client.close()
  }

  def createKafkaConsumer(topic: String): KafkaConsumer[String, String] = {
    val groupId = "nytime_consumer_group"
    val properties = new Properties()
    val offsetStra = "earliest"

    // create config
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetStra)
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20")

    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(Collections.singleton(topic))
    consumer
  }

  def createClient: RestHighLevelClient = {
    val hostname = "localhost"
    val username = ""
    val password = ""

    //    val crednetialsProvider: CredentialsProvider = new BasicCredentialsProvider()

    //    crednetialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password))
    val builder = RestClient.builder(new HttpHost(hostname, 9200, "http"))
    //      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback {
    //        override def customizeHttpClient(httpAsyncClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder =
    //          httpAsyncClientBuilder.setDefaultCredentialsProvider(crednetialsProvider)
    //      })

    val client = new RestHighLevelClient(builder)
    client
  }
}