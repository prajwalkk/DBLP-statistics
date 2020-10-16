name := "DBLP-statistics"
version := "0.1"
scalaVersion := "2.13.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(

  // https://mvnrepository.com/artifact/org.scalatest/scalatest
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,

  // https://mvnrepository.com/artifact/com.typesafe/config
  "com.typesafe" % "config" % "1.3.4",

  // https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,

  // https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",

  "com.github.pureconfig" %% "pureconfig" % "0.13.0",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",

  "org.apache.hadoop" % "hadoop-common" % "3.3.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.0",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M2"

)

mainClass in(Compile, run) := Some("com.prajwalkk.hw2.MapReduceJobs.JobDriver")
mainClass in assembly := Some("com.prajwalkk.hw2.MapReduceJobs.JobDriver")