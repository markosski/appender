package appender.batch.impl

import java.io._

import appender.batch.{BatchRecord, BatchStore}
import appender.format.Codec

import scala.util.{Failure, Success, Try}
import appender.util.implicits._

class BatchStoreLocal(val codec: Codec, val path: String) extends BatchStore {
  logger.info(s"Creating BatchStore using path $path")

  def read(partition: String, batchNum: Int): BatchRecord = {
    val fileName = batchNumToFileName(partition, batchNum)
    logger.info(s"Reading batch with partition $partition and batchNum $batchNum")

    val bis = new BufferedInputStream(new FileInputStream(path / partition / fileName))
    val bytes = new Array[Byte](1024)
    var len = 0

    val os = new ByteArrayOutputStream()
    while ( {
      len = bis.read(bytes); len
    } > 0) os.write(bytes, 0, len)
    bis.close()

    BatchRecord(partition, batchNum, os)
  }

  def partitions: Seq[String] = {
    path.toFile.listFiles.filterNot(x => x.getName.startsWith(".")).map(x => x.getName)
  }

  def batches(partition: String): (Seq[String], Seq[Int]) = {
    val filePath = new File(path + "/" + partition)
    val list = filePath.listFiles.sortBy(_.getName)

    Tuple2(
      for (f <- list) yield f.getName,
      for (f <- list) yield parseBatchNum(f.getName)
    )
  }

  def write(batchRecord: BatchRecord, partition: String): Unit = {
    val fileName = Try(batchNumToFileName(partition, batchRecord.num)) match {
      case Success(s) => s
      case Failure(err) => s"${batchRecord.num}.${codec.ext}"
    }

    val file = new File(path / partition / fileName)

    if (!file.exists) {
      logger.info(s"Batch file $file does not exist, will attempt to created.")
      file.getParentFile.mkdirs
      file.createNewFile
    }

    val fos = new FileOutputStream(file)
    batchRecord.data.writeTo(fos)
    fos.close()
  }
}
