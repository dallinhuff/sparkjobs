ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "sparkjobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.13" % "3.5.0",
      "org.apache.spark" % "spark-sql_2.13" % "3.5.0"
    )
  )
