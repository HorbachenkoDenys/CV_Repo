name := "RDD"

version := "0.1"

scalaVersion := "2.11.1"

val sparkVersion = "2.2.1"

libraryDependencies ++= Seq (

  "org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion

)