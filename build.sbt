import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "appender",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += depConfig,
    libraryDependencies += depS3,
    libraryDependencies ++= depLogging,
    libraryDependencies += avro
  )
