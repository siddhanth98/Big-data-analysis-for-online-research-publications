fork := true
// Include only src/main/scala in the compile configuration
Compile / unmanagedSourceDirectories := (Compile / scalaSource).value :: Nil

// Include only src/test/scala in the test configuration
Test / unmanagedSourceDirectories := (Test / scalaSource).value :: Nil

lazy val hadoopCore = "org.apache.hadoop" % "hadoop-core" % "1.2.1" % "provided"
lazy val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "1.3.0"
lazy val logbackCore = "ch.qos.logback" % "logback-core" % "1.2.3"
lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.2.3"
lazy val slf4j = "org.slf4j" %"slf4j-api" %"1.7.30" % "test"
lazy val typesafe = "com.typesafe" % "config" % "1.4.0"

lazy val thisProject = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(hadoopCore, scalaXml, logbackCore, logbackClassic, slf4j, typesafe)
  )

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
