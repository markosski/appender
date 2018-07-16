package appender.batch

import scala.util.{Success, Failure, Try}


/**
  * - needs to know how to decode bytes to count number of records.
  * - also needs a utility that will iterate and create records
  */
trait BatchReader {
    val numPattern = "[0-9]+".r

    def parseBatchNum(fileName: String): Int = Try(numPattern.findFirstIn(fileName).get) match {
        case Success(s) => s.toInt
        case Failure(err) => throw new Exception(err)
    }

    def getPartitionedBatch(partition: String, fileName: String): BatchRecord

    /**
      * List all partitions
      * @return
      */
    def partitions: Seq[String]

    /**
      * List number of batches in partition
      * @param partition
      * @return
      */
    def batches(partition: String): (Seq[String], Seq[Int])
}
