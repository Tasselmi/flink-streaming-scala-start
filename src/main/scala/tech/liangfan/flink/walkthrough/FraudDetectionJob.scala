package tech.liangfan.flink.walkthrough

import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.source.TransactionSource

object FraudDetectionJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val transactions = env
      .addSource(new TransactionSource)
      .name("transactions")

    val alerts = transactions
      .keyBy(tran => tran.getAccountId)
      .process(new FraudDetector)
      .name("fraud-detector")

    alerts.addSink(new AlertSink).name("log-fraud-id")
    alerts.print().name("stdout-fraud-id")

    env.execute("fraud-detection-job")
  }

}
