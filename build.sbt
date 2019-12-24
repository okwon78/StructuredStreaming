name := "StructuredStreaming"

version := "0.2"

scalaVersion := "2.11.12"

//lazy val commonSettings = Seq(
//  version := "0.1-SNAPSHOT",
//  organization := "org.amorepacific",
//  scalaVersion := "2.11.12",
//  test in assembly := {}
//)
//

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4"

// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "7.4.2"




assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case "application.conf" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

