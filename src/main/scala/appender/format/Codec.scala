package appender.format

trait Codec {
  val ext: String

  override def toString: String = ext

  def encode(msg: String): Array[Byte]
}

