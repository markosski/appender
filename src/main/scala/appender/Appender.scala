package appender

import appender.batch.{BatchMemoryBuffer, BatchStore}
import org.slf4j.LoggerFactory

class Appender(batchStore: BatchStore) {
  val batchBuffer = new BatchMemoryBuffer(batchStore)
  private val logger = LoggerFactory.getLogger(getClass)

  def put(partName: String, partVal: String, msg: String): Unit = {
    val partition = s"$partName=$partVal"

    logger.info(s"Attempting to insert message $msg with partition $partition")
    batchBuffer.put(partition, msg)
  }

  def flush = {
    batchBuffer.materializeAll
  }
}
