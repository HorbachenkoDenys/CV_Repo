name := "SQL"

version := "0.1"

scalaVersion := "2.11.1"

val sparkVersion = "2.3.2"

libraryDependencies ++= Seq (

  "org.apache.spark" %% "spark-core" % sparkVersion,

  "org.apache.spark" %% "spark-sql" % sparkVersion

)