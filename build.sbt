name := "KKIA Big Data Project"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-mllib" % "3.5.1"
)
ThisBuild / javaHome := Some(file("C:/Program Files/Java/jdk-11"))