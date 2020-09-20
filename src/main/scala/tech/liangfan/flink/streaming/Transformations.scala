package tech.liangfan.flink.streaming

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark
import play.api.libs.json._

/**
 * @author liangfan
 * @groupname com.bbim.bigdata
 * @create_date 2020-09-12
 * @description functionality and usage of class
 *
 */
object Transformations {

    val js1 = """{"name": "liangfan", "age": 11.7, "isTop": true}"""
    val js2 = """{"name": "liangfan", "age": "11.7", "isTop": true}"""
    //val res = Json.parse(js)

    implicit def jsToHolder(res: JsLookupResult): AnyValueJsonHolder =
        new AnyValueJsonHolder(res)

    class AnyValueJsonHolder(res: JsLookupResult) {
        def anyValToString(): String = res.asOpt[String] match {
            case Some(s) => s
            case None => res.asOpt[JsNumber] match {
                case Some(jn) => jn.toString()
                case None => res.asOpt[JsBoolean] match {
                    case Some(jb) => jb.toString()
                    case None => res.toString
                }
            }
        }
    }

    val res = (Json.parse(js1) \ "age").anyValToString()

    (Json.parse(js2) \ "age").as[String]

    case class Person(name: String, age: Int)

    def main(args: Array[String]): Unit = {

        val input: DataStream[(Int, Int, Int)] = streamEnv
            .fromElements((1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

        val result = input.keyBy(0).sum(1)
        val result2 = input.keyBy(0).minBy(1)

        result.print()
        //result2.print()


        val input3 = streamEnv
            .fromElements(("en", List("tea")), ("fr", List("vin")), ("en", List("cake")))
        val result3 = input3.keyBy(0).reduce((a, b) => (a._1, a._2 ::: b._2))
        result3.print()
        result3.broadcast
        result.shuffle
        result.rebalance
        result.rescale
        
        //result.partitionCustom()

        println(s"parallelism: ${streamEnv.getParallelism}")

        streamEnv.getConfig.setAutoWatermarkInterval(500)

        streamEnv.execute()
    }

    class Tuple2ToPersonMapper extends MapFunction[(String, Int), Person] with ResultTypeQueryable[Person] {
        override def map(value: (String, Int)): Person = ???

        override def getProducedType: TypeInformation[Person] = ???
    }

    class PeriodicAssigner extends AssignerWithPeriodicWatermarks[Person] {
        override def getCurrentWatermark: Watermark = ???

        override def extractTimestamp(element: Person, previousElementTimestamp: Long): Long = ???
    }

    class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[Person] {

        private val bound = 60 * 1000

        override def checkAndGetNextWatermark(lastElement: Person, extractedTimestamp: Long): Watermark = {
            if (lastElement.age > 100) new Watermark(extractedTimestamp - bound)
            else null
        }

        override def extractTimestamp(element: Person, previousElementTimestamp: Long): Long = {
            element.age.toLong
        }

    }

}
