package tech.liangfan.flink.tutorials


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object WindowWordCount {

    def main(args: Array[String]): Unit = {
        val params = ParameterTool.fromArgs(args)
        val hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
        val port = if (params.has("port")) params.getInt("port") else 9999

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

        val windowCounts = text
            .flatMap(w => w.split("\\s"))
            .map(w => WordWithCount(w, 1))
            .keyBy("word")
            .timeWindow(Time.seconds(5))
            .sum("count")

        windowCounts.print().setParallelism(1)

        env.execute("window-word-count")
    }

    case class WordWithCount(word: String, count: Long)
}
