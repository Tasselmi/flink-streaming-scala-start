package tech.liangfan.flink

import org.apache.flink.streaming.api.scala._

object IterationExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val someIntegers: DataStream[Long] = env.generateSequence(0, 1000)

    val iteratedStream = someIntegers.iterate { iteration =>
      val minusOne = iteration.map(l => l - 1)
      val aboveZero = minusOne.filter(_ > 0)
      val belowZero = minusOne.filter(_ <= 0)
      //第一部分会不停反馈给起始位置进行迭代，第二部分会向下传递给downStream
      //stepfunction: initialStream => (feedback, output)
      (aboveZero, belowZero)
    }

    iteratedStream.print().setParallelism(1)

    env.execute("iteration-example")
  }

}
