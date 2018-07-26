package appender

import com.typesafe.config.{Config, ConfigFactory}

object config {
  val conf: Config = ConfigFactory.load()

  val localBufferDir: String = conf.getString("localBufferDir")
  val batchSize: Int = conf.getInt("batchSize")
}
