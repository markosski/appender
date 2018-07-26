package appender.batch

import java.io.ByteArrayOutputStream

import appender.config

/**
  * - needs to be able to iterate over buffer and produce records or not?
  * @param partition
  * @param num
  * @param data
  */
case class BatchRecord(
                        partition: String,
                        num: Int,
                        data: ByteArrayOutputStream)

object BatchRecord {
  def empty(partition: String, num: Int): BatchRecord = {
    BatchRecord(partition, num, new ByteArrayOutputStream())
  }
}
