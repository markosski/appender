package appender.batch

import appender.config
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * - should know number of records in a batch.
  * - materialize if number of records in batch is met
  *
  * @param batchStore
  */
class BatchMemoryBuffer(batchStore: BatchStore) {
  protected val batchMap: mutable.Map[String, BatchRecord] = mutable.Map()
  protected val logger = LoggerFactory.getLogger(getClass)

  syncBatches

  /**
    * Get the buffer. Only most recent batch is store for partition.
    *
    * @param partition
    * @return
    */
  def getBatch(partition: String): Option[BatchRecord] = batchMap.get(partition)

  def put(partition: String, msg: String, tried: Int = 0): Unit = {
    if (tried == 3) throw new Exception("Error while creating partition.")

    getBatch(partition) match {
      case Some(batch) => {
        // Check if new batch buffer exceeds max batch size
        val encodedMessage = batchStore.codec.encode(msg)

        if (encodedMessage.length + batch.data.size > config.batchSize) {
          logger.info(s"Batch buffer full, will materialize current batch $batch in partition $partition.")
          materialize(partition)

          logger.info(s"Creating new empty batch in partition $partition.")
          val newBatch = BatchRecord.empty(partition, batch.num + 1)
          batchStore.write(newBatch, partition)

          newBatch.data.write(
            encodedMessage
          )

          batchMap.put(partition, newBatch)
        } else {
          logger.info(s"Adding message to current batch $batch in partition $partition.")
          batch.data.write(
            encodedMessage
          )
        }
      }
      case None => {
        logger.info(s"Creating new empty batch in partition $partition.")
        val newBatch = BatchRecord.empty(partition, 0)
        batchStore.write(newBatch, partition)

        batchMap.put(partition, newBatch)
        put(partition, msg, tried + 1)
      }
    }
  }

  /**
    * Get latest batches for each partition.
    */
  def syncBatches: Unit = {
    val partitions = batchStore.partitions
    logger.info(s"Running syncBatches on partition $partitions")

    for (p <- partitions) {
      val batches = batchStore.batches(p)

      if (batches._1.nonEmpty) {
        val batchFileName = batches._2.last
        val data = batchStore.read(p, batchFileName)
        batchMap.put(p, data)
      }
    }
  }

  def materialize(partition: String) = {
    getBatch(partition) match {
      case Some(batch) => {
        logger.info(s"Materializing batch in partition $partition.")
        batchStore.write(batch, partition)
      }
      case None => {
        throw new Exception(s"Error materializing, partition $partition does not exist.")
      }
    }
  }

  def materializeAll = {
    for (p <- batchStore.partitions) {
      materialize(p)
    }
  }
}
