package nyc

import java.util.concurrent.BlockingQueue

class NYCClientBuilder {
  var msgQueue: BlockingQueue[String] = null
  var api: String = null

  def addQueue(msgQueue: BlockingQueue[String]): NYCClientBuilder = {
    this.msgQueue = msgQueue
    this
  }

  def addApi(apiKey: String): NYCClientBuilder = {
    api = apiKey
    this
  }

  def build(): NYClient = new NYClient(msgQueue, api)

}
