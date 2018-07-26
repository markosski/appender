import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val depConfig = "com.typesafe" % "config" % "1.3.2"
  lazy val depLogging = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  )
  lazy val depS3 = "com.amazonaws" % "aws-java-sdk-s3" % "1.11.371"
  lazy val avro = "org.apache.avro" % "avro" % "1.8.2"
}
