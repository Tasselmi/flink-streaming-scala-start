package tech.liangfan.flink.walkthrough

import java.lang

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

    @transient
    private var flagState: ValueState[java.lang.Boolean] = _
    @transient
    private var timerState: ValueState[java.lang.Long] = _

    override def processElement(
        transaction: Transaction,
        context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
        collector: Collector[Alert]): Unit = {

        val lastTransactionWasSmall: lang.Boolean = flagState.value()

        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
                val alert = new Alert
                alert.setId(transaction.getAccountId)
                collector.collect(alert)
            }

            cleanUp(context)
        }

        if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
            flagState.update(true)

            val timer = context.timerService().currentProcessingTime() + FraudDetector.ONE_MINUTE
            context.timerService().registerProcessingTimeTimer(timer)
            timerState.update(timer)
        }
    }

    override def open(parameters: Configuration): Unit = {
        val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
        flagState = getRuntimeContext.getState(flagDescriptor)

        val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
        timerState = getRuntimeContext.getState(timerDescriptor)
    }

    override def onTimer(
        timestamp: Long,
        ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
        out: Collector[Alert]): Unit = {

        timerState.clear()
        flagState.clear()
    }

    private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
        val timer = timerState.value()
        ctx.timerService().deleteProcessingTimeTimer(timer)

        timerState.clear()
        flagState.clear()
    }

}

object FraudDetector {
    val SMALL_AMOUNT = 1.00
    val LARGE_AMOUNT = 500.00
    val ONE_MINUTE: Long = 60 * 1000L
}
