package tech.liangfan.flink.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners._

object TimeAndWatermarks {

    case class Record(time: Long, key: String, amount: Double)

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "kafka-test-01")
        properties.setProperty("enable.auto.commit", "false")

        val source: FlinkKafkaConsumer010[String] =
            new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), properties)
        source.setStartFromLatest()
        source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[String] {
            override def extractAscendingTimestamp(element: String): Long =
                element.split(",")(0).toLong
        })

        //source.setStartFromGroupOffsets()  默认如此
        val stream = env.addSource(source)
//            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[String] {
//                override def extractAscendingTimestamp(element: String): Long =
//                    element.split(",")(0).toLong
//            })

            .map(s => s.split(","))
            .map(arr => Record(arr(0).toLong, arr(1), arr(2).toDouble))
            .assignAscendingTimestamps(_.time)
//            .assignTimestampsAndWatermarks(
//                new BoundedOutOfOrdernessTimestampExtractor[Record](Time.milliseconds(500)) {
//                    override def extractTimestamp(element: Record): Long =
//                        element.time
//            })
            .keyBy("key")
            //window size 统计的区间, window slide 更新频率, long offset
            //.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(3)))
            //.allowedLateness(Time.seconds(10))
            //.timeWindow(Time.seconds(10))
            .timeWindow(Time.seconds(3000))
            //.window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .sum("amount")


//        AssignerWithPeriodicWatermarks
//        AssignerWithPunctuatedWatermarks


        stream.print()

        env.execute()
    }

}
