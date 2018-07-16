package appender.batch.impl.fs

import java.io._

import appender.batch.{BatchReader, BatchRecord}

class BatchReaderLocal(path: String) extends BatchReader {
    val filePath = new File(path)

    assert(filePath.isDirectory, s"$filePath does not exist")

    def getPartitionedBatch(partition: String, fileName: String): BatchRecord = {
        val is = new BufferedInputStream(new FileInputStream(s"$filePath/$partition/$fileName"))
        val bytes = new Array[Byte](1024)
        var len = 0

        val os = new ByteArrayOutputStream()
        while ( { len = is.read(bytes); len } > 0) os.write(bytes, 0, len)
        is.close

        BatchRecord(partition, parseBatchNum(fileName), os, 0)
    }

    def partitions: Seq[String] = {
        filePath.listFiles.map(x => x.getName)
    }

    def batches(partition: String): (Seq[String], Seq[Int]) = {

        val filePath = new File(path + "/" + partition)

        (
            for (f <- filePath.listFiles) yield f.getName,
            for (f <- filePath.listFiles) yield parseBatchNum(f.getName)
        )
    }
}
