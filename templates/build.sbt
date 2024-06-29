name := "{{general.name}}"

version := "1.0"

scalaVersion := "{{scala.version}}"

val currentDirectory = new java.io.File(".").getCanonicalPath

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-hadoop-cloud" % "3.5.1" from ("file://" + currentDirectory + "/docker/jars/spark-hadoop-cloud_2.12-3.5.1.jar")
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4" from ("file://" + currentDirectory + "/docker/jars/hadoop-aws-3.3.4.jar")
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262" from ("file://" + currentDirectory + "/docker/jars/aws-java-sdk-bundle-1.12.262.jar")
//libraryDependencies += "com.amazonaws" % "aws-java-sdk-core" % "1.12.262" from ("file://" + currentDirectory + "/docker/jars/aws-java-sdk-core-1.12.262.jar")
//libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.12.262" from ("file://" + currentDirectory + "/docker/jars/aws-java-sdk-s3-1.12.262.jar")
libraryDependencies += "com.databricks" % "spark-sql-perf_2.12" % "0.5.1" from ("file://" + currentDirectory + "/docker/jars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar")
libraryDependencies += "com.lihaoyi" %% "upickle" % "3.3.1" from ("file://" + currentDirectory + "/docker/jars/upickle_2.12-3.3.1.jar")


