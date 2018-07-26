package appender

import appender.batch.impl.{BatchStoreLocal, BatchStoreS3}
import appender.format.{AvroCodec, TextCodec}
import org.apache.avro.generic.{GenericData, GenericRecord}

object Test extends App {
  def local = {
    val batchStore = new BatchStoreLocal(new TextCodec("txt"), "src/main/resources/data/test.txt")
    val ap = new Appender(batchStore)

    for (i <- 0 until 10) {
      ap.put("date", "052018", "Marcin Kossakowski")
      ap.put("date", "062018", "Melissa Maida")
    }

    ap.flush

    print("done with local batch store")
  }

  def avro = {
    val schemaJson =
      """{
        |"namespace": "appender",
        |"type": "record",
        |"name": "Entity",
        |"fields": [
        |{
        | "name": "name",
        | "type": "string"
        |},
        |{
        | "name": "age",
        | "type": "int"
        |}]
        |}
      """.stripMargin

    val batchStore = new BatchStoreLocal(new AvroCodec(schemaJson), "src/main/resources/data/test.avro")
    val ap = new Appender(batchStore)

    for (i <- 0 until 10) {
      ap.put("date", "052018", """{"name": "marcin", "age": 36}""")
      ap.put("date", "062018", """{"name": "melissa", "age": 34}""")
    }

    ap.flush
  }

  def s3 = {
    val batchStore = new BatchStoreS3(new TextCodec("csv"), "data/test1.csv", "appender")
    val ap = new Appender(batchStore)

    for (i <- 0 until 10) {
      ap.put("date", "052018", "Marcin Kossakowski")
      ap.put("date", "062018", "Melissa Maida")
    }

    ap.flush

    print("done with s3 batch store")
  }

//  local
//  s3
  avro
}
