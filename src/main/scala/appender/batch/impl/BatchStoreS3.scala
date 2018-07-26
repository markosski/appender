package appender.batch.impl

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import appender.batch.{BatchRecord, BatchStore}
import appender.format.Codec
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import appender.util.implicits._
import org.slf4j.LoggerFactory

class S3Client {
  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withRegion("us-east-1")
    .withCredentials(new ProfileCredentialsProvider())
    .build()

  def getClient(msg: String = "") = {
    logger.info(s"Calling S3 client: $msg")
    client
  }
}

class BatchStoreS3(val codec: Codec, val path: String, bucket: String) extends BatchStore {
  private val delimiter = "/"
  private val s3: S3Client = new S3Client
  /**
    * Write batch to storage.
    *
    * @param batchRecord
    */
  override def write(batchRecord: BatchRecord, partition: String): Unit = {
    val fileName = Try(batchNumToFileName(partition, batchRecord.num)) match {
      case Success(s) => s
      case Failure(err) => s"${batchRecord.num}.${codec.ext}"
    }

    val key = path / partition / fileName

    val meta = new ObjectMetadata()
    meta.setContentLength(batchRecord.data.size)
    meta.setContentType("application/x-binary")

    s3.getClient("Write object")
      .putObject(bucket, key, new ByteArrayInputStream(batchRecord.data.toByteArray), meta)
  }

  override def read(partition: String, batchNum: Int): BatchRecord = {
    val obj = s3.getClient("Get contents of object")
      .getObject(bucket, path / partition / {batchNumToFileName(partition, batchNum)})

    val os = new ByteArrayOutputStream()
    val is = obj.getObjectContent
    val bytes = new Array[Byte](1024)
    var len = 0

    while ( {len = is.read(bytes); len } > 0)
      os.write(bytes, 0, len)
    is.close()

    BatchRecord(partition, batchNum, os)
  }

  /**
    * List all partitions
    *
    * @return
    */
  override def partitions: Seq[String] = {
    val resp = s3.getClient("List partitions")
      .listObjects(bucket, path).getObjectSummaries.iterator()
    val pathGroupsSize = path.split(delimiter).length
    var foundPartitions = List[String]()
    val partitionLookup = mutable.Set[String]()

    while (resp.hasNext) {
      val obj = resp.next()
      val objPathGroups = obj.getKey.split(delimiter)

      if (objPathGroups.length > pathGroupsSize + 1 && !partitionLookup.contains(obj.getKey.split(delimiter)(pathGroupsSize))) {
        foundPartitions = foundPartitions :+ obj.getKey.split(delimiter)(pathGroupsSize)
        partitionLookup += obj.getKey.split(delimiter)(pathGroupsSize)
      }
    }
    foundPartitions
  }

  /**
    * List number of batches in partition
    *
    * @param partition
    * @return
    */
  override def batches(partition: String): (Seq[String], Seq[Int]) = {
    val partitionPath = path / partition / ""
    val resp = s3.getClient("List batches in partition")
      .listObjects(bucket, partitionPath).getObjectSummaries.iterator
    val partitionPathSize = partitionPath.split(delimiter).length
    var batchNum = List[Int]()
    var batchFile = List[String]()

    while (resp.hasNext) {
      val obj = resp.next()
      val objPathGroups = obj.getKey.split(delimiter)

      if (objPathGroups.length > partitionPathSize) {
        batchNum = batchNum :+ parseBatchNum(objPathGroups(partitionPathSize))
        batchFile = batchFile :+ objPathGroups(partitionPathSize)
      }
    }

    Tuple2(batchFile, batchNum)
  }
}
