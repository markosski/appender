package appender.util

import scala.language.implicitConversions
import java.io.File

object implicits {
  implicit def addPathSeparator(s: String) = new {
    val sep = java.io.File.separatorChar

    def /(z: String): String = {
      z match {
        case z if s == "~" => System.getProperty("user.home") + sep + z
        case _ => s + sep + z
      }
    }
    def /(z: File): File = {
      new File(s + sep + z.toString)
    }
    def toFile = new File(s)
  }
}
