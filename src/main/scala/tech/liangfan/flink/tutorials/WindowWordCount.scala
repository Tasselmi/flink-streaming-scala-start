package tech.liangfan.flink.tutorials


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import tech.liangfan.flink.streaming.TimeAndWatermarks.Record


object WindowWordCount {

    def main(args: Array[String]): Unit = {
        val params = ParameterTool.fromArgs(args)
        val hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
        val port = if (params.has("port")) params.getInt("port") else 9999

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

        val windowCounts = text
            .flatMap(w => w.split("\\s"))
            .map(s => s.split(","))
            .map(arr => Record(arr(0).toLong, arr(1), arr(2).toDouble))
            .assignAscendingTimestamps(_.time)
            .keyBy("key")
            .timeWindow(Time.seconds(3))
            .sum("amount")
            //.map(w => WordWithCount(w, 1))
//            .keyBy("word")
//            .timeWindow(Time.seconds(5))
//            .sum("count")

        windowCounts.print().setParallelism(1)

        env.execute("window-word-count")
    }

    case class WordWithCount(word: String, count: Long)

    case class Record(time: Long, key: String, amount: Double)
}
