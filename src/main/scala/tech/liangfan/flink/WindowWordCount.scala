package tech.liangfan.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("locaohost", 9999)

    val counts = text
      .flatMap(_.toLowerCase.split("\\W+").filter(_.nonEmpty))
      .map(s => (s, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    counts.print()

    env.execute("window-word-count")
  }
}
