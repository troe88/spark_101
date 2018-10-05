name := "big data #101"

version := "0.1"

scalaVersion := "2.11.9"


val sparkVersion = "2.2.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)