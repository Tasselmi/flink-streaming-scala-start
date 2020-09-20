name := "flink-streaming-scala-start"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "1.10.1"

libraryDependencies ++= Seq(
	"org.apache.flink" % "flink-core" % flinkVersion,
	"org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
	"org.apache.flink" %% "flink-scala" % flinkVersion,
	"org.apache.flink" % "flink-walkthrough-common_2.11" % flinkVersion,
	"org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion,
	"com.typesafe.play" %% "play-json" % "2.7.4",
	"redis.clients" % "jedis" % "3.3.0",
	"com.google.code.gson" % "gson" % "2.8.6",
	"org.javatuples" % "javatuples" % "1.2",
	"org.apache.commons" % "commons-csv" % "1.8"
)