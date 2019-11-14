


name := "chombo-spark"

organization := "org.chombo"

version := "1.0"

scalaVersion := "2.12.0"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0-preview" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.0.0-preview",
  //"org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.3",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.9.4",
  "org.apache.lucene" % "lucene-core" % "4.4.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "4.4.0",
  "junit" % "junit" % "4.7" % "test",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "com.typesafe" % "config" % "1.2.1",
  "mawazo" %% "chombo" % "1.0"
)

