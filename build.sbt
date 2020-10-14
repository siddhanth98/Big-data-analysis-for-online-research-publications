fork := true
// Include only src/main/scala in the compile configuration
Compile / unmanagedSourceDirectories := (Compile / scalaSource).value :: Nil

// Include only src/test/scala in the test configuration
Test / unmanagedSourceDirectories := (Test / scalaSource).value :: Nil

lazy val hadoopCore = "org.apache.hadoop" % "hadoop-core" % "1.2.1" % "provided"
lazy val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "1.3.0"

lazy val thisProject = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(hadoopCore, scalaXml)
  )

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
