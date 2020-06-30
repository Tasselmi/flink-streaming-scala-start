name := "flink-streaming-scala-start"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "1.10.1"

libraryDependencies ++= Seq(
	"org.apache.flink" % "flink-core" % flinkVersion,
	"org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
	"org.apache.flink" %% "flink-scala" % flinkVersion,
	"org.apache.flink" % "flink-walkthrough-common_2.11" % flinkVersion
)