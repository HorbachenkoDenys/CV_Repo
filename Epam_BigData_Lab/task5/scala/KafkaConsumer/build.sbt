import sbt.Keys.libraryDependencies

name := "com.horbachenkodenis.MyConsumer"

version := "0.1"

scalaVersion := "2.11.1"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.11.1" % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"
)
libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.0" % "provided"
