package appender

import appender.batch.{BatchBufferMemory, BatchWriter}
import appender.encoder.Encoder

class AppenderWriter(batchWriter: BatchWriter, encoder: Encoder) {
    val memoryBuffer: BatchBufferMemory = ???

    def write(mssg: String) = ???
}
