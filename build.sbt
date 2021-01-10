name := "SBTProject"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.4.1"

// https://mvnrepository.com/artifact/org.apache.hive/hive-common
//libraryDependencies += "org.apache.hive" % "hive-common" % "2.0.0"  // this works, but might not be needed

// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
//libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.6.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "it,test"


// https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.11.0" % "it,test"