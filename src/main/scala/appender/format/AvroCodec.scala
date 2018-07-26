package appender.format

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

class AvroCodec(schemaJson: String) extends Codec {
  val ext = "avro"

  private val schema = new Schema.Parser().parse(schemaJson)
  private val writer = new GenericDatumWriter[GenericRecord](schema)
  private val reader = new GenericDatumReader[GenericRecord](schema)

  def encode(msg: String): Array[Byte] = {
//    val record: GenericRecord = new GenericData.Record(schema)

    val is = new ByteArrayInputStream(msg.getBytes())
    val decoder = DecoderFactory.get().jsonDecoder(schema, is)

    val out = new ByteArrayOutputStream()
    val record = reader.read(null, decoder)

    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush() // !
    out.toByteArray
  }
}
