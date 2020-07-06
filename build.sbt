name := "tmc-test"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.816"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.1"


libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"