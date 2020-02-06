package nyc

import java.util.concurrent.BlockingQueue

import org.json.JSONObject
import org.slf4j.LoggerFactory


class NYClient(msgQueue: BlockingQueue[String], api: String) {
  private var isQueryCompleted = false
  val logger = LoggerFactory.getLogger(classOf[NYClient].getName)
  val limit: Int = 100
  var offset = 0
  val latency = 10000

  def connect(): Unit = {
    val queryThread = new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          try {
            val url = s"https://api.nytimes.com/svc/news/v3/content/all/all.json?limit=$limit&offset=${offset}&api-key=${api}"
            val result = scala.io.Source.fromURL(url).mkString
            val obj: JSONObject = new JSONObject(result)
            val resultArray = obj.getJSONArray("results")
            offset += resultArray.length()

            logger.info(s"Query return size: ${resultArray.length()}")
            (0 to resultArray.length() - 1).foreach(index => {
              val resultObject = resultArray.getJSONObject(index)
              val resultString = resultObject.toString
              msgQueue.add(resultString)
            })
          } catch {
            case e: Exception => logger.info("No more result found in NYTimes")
            case _ => logger.warn("Something wrong in parsing result")
          }
          Thread.sleep(latency)
        }
      }
    })
    queryThread.start()
  }

  def isDone: Boolean = false
}
