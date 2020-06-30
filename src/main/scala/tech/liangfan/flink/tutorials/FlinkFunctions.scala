package tech.liangfan.flink.tutorials

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._

object FlinkFunctions {

    case class WordCount(word: String, count: Int)

    class MyMapFunction extends RichMapFunction[String, Int] {
        override def map(value: String): Int = value.toInt
    }

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val input1 = env.fromElements(
            WordCount("liang", 11),
            WordCount("fan", 18)
        )

        val input2 = env.fromElements("100", "200")
        val res2 = input2.map(new MyMapFunction())
        res2.print()

        var counter = new IntCounter()


        env.execute("test-map-functions")
    }

}
