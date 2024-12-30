import Dependencies._

ThisBuild / scalaVersion := "2.13.15"
ThisBuild / version := "1.0.0"
ThisBuild / organization := "be.big-data-processing"
ThisBuild / organizationName := "CCBDP"

lazy val root = (project in file("."))
  .settings(
    name := ".",
    libraryDependencies += munit % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.4",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.4"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
