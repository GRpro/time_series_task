name := "time_series_task"

version := "1.0"

scalaVersion := "2.12.6"

// https://mvnrepository.com/artifact/commons-cli/commons-cli
libraryDependencies += "commons-cli" % "commons-cli" % "1.4"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

mainClass in assembly := Some("Aggregator")
assemblyJarName in assembly := "aggregator.jar"