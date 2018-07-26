package appender.format

class TextCodec(val ext: String) extends Codec {
  def encode(msg: String): Array[Byte] = {
    val newData = msg.getBytes.toBuffer
    newData.append('\n'.toByte)
    newData.toArray
  }
}
