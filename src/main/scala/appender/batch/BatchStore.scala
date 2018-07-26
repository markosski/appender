package appender.batch

import appender.format.{Codec}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


/**
  * - needs to know how to decode bytes to count number of records.
  * - also needs a utility that will iterate and create records
  */
trait BatchStore {
  val path: String
  val codec: Codec

  protected val logger = LoggerFactory.getLogger(getClass)
  private val numPattern = "^[0-9]+".r

  protected def parseBatchNum(fileName: String): Int = Try(numPattern.findFirstIn(fileName).get) match {
    case Success(s) => s.toInt
    case Failure(err) => throw new Exception(s"Could not parse batch number from batch file name $fileName.")
  }

  /**
    * Recover file name based on partition name and batch number.
    *
    * @param partition
    * @param batchNum
    * @return
    */
  protected def batchNumToFileName(partition: String, batchNum: Int): String = {
    val partBatches = batches(partition)

    partBatches._2.zipWithIndex.filter(_._2 == batchNum).toList match {
      case Nil => throw new Exception(s"Batch $batchNum does not exist.")
      case head :: _ => partBatches._1(head._1)
    }
  }

//  def format: FileFormat = Codec.fromString{
//    val lastSegment = path.split("/").last
//
//    if (lastSegment.contains(".")) {
//      lastSegment.split("\\.").last
//    } else {
//      ""
//    }
//  }

  /**
    * Write batch to storage.
    * @param batchRecord
    */
  def write(batchRecord: BatchRecord, partition: String): Unit

  def read(partition: String, batchNum: Int): BatchRecord

  /**
    * List all partitions
    *
    * @return
    */
  def partitions: Seq[String]

  /**
    * List batches within partition.
    * First element contains file name, second extracted number.
    * Both sequences MUST use ascending order.
    *
    * @param partition
    * @return
    */
  def batches(partition: String): (Seq[String], Seq[Int])
}
