package partitioner

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

import scala.util.matching.Regex

class SimpleCustomPartitioner extends Partitioner {
  val pattern: Regex = "([0-9]+)".r

  def partition(topic: String,
                key: Any,
                keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte],
                cluster: Cluster): Int = {
    val customPartition = pattern.findFirstMatchIn(key.toString)
    customPartition.get.toString().toInt % 3
  }

  override def close(): Unit = None

  override def configure(configs: util.Map[String, _]): Unit = None
}
