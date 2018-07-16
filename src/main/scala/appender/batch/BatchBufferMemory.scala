package appender.batch

import java.io.{BufferedOutputStream, ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer

import scala.collection.mutable

/**
  * - should know number of records in a batch.
  * - materialize if number of records in batch is met
  *
  * @param reader
  * @param appendLogPath
  */
class BatchBufferMemory(reader: BatchReader, appendLogPath: String) {
    private val buffer: mutable.Map[String, BatchRecord] = mutable.Map()

    syncBatches

    /**
      * Get the buffer.
      * @param partition
      * @return
      */
    def getBuffer(partition: String): Option[BatchRecord] = buffer.get(partition)

    def put(partition: String, msg: Array[Byte]): Unit = {
        getBuffer(partition) match {
            case Some(buff) => buff.data.write(msg)
            case None => Unit
        }
    }

    /**
      * Get tail batches, only if not full.
      */
    private def syncBatches: Unit = {
        val partitions = reader.partitions

        for (p <- partitions) {
            val batches = reader.batches(p)
            val batchFileName = batches._1.tail.head
            val data = reader.getPartitionedBatch(p, batchFileName)

            buffer.put(p, data)
        }
    }
}
