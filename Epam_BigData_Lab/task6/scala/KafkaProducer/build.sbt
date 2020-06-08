import sbt.Keys.libraryDependencies

name := "com.horbachenkodenis.KafkaProducer"

version := "0.1"

scalaVersion := "2.11.1"

val kafkaVersion = "2.0.0"

libraryDependencies ++= Seq (
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)
libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.0"

