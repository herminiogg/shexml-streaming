ThisBuild / organization := "com.herminiogarcia"

lazy val root = (project in file("."))
  .settings(
    name := "shexml-streaming",
    version := "0.0.1",
    scalaVersion := "3.3.6",
    crossScalaVersions := Seq("2.12.20", "2.13.16", "3.3.6"),
    libraryDependencies ++= Seq(
      "info.picocli" % "picocli" % "4.7.7",
      "com.herminiogarcia" %% "shexml" % "0.6.1",
      "com.softwaremill.sttp.client4" %% "core" % "4.0.9",
      "com.softwaremill.sttp.client4" %% "monix" % "4.0.9",
      "io.monix" %% "monix" % "3.4.0",
      "io.reactivex.rxjava3" % "rxjava" % "3.1.8",
      "info.picocli" % "picocli" % "4.7.7",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "org.scalatest" %% "scalatest" % "3.2.9" % "test"
    )
  )
