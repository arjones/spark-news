import AssemblyKeys._ // put this at the top of the file

name := "SparkNews"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.twitter4j" % "twitter4j-core" % "3.0.6",
  "org.twitter4j" % "twitter4j-stream" % "3.0.6",
  "org.apache.spark" %% "spark-core" % "1.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.0.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.0.0"
)

// resourceDirectory in Compile := baseDirectory.value / "resources"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
