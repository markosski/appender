package appender.batch

import java.io.ByteArrayOutputStream

/**
  * - needs to be able to iterate over buffer and produce records or not?
  * @param partition
  * @param num
  * @param data
  * @param recordCount
  */
case class BatchRecord(partition: String, num: Int, data: ByteArrayOutputStream, recordCount: Int)
