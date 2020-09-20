package tech.liangfan.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author liangfan
 * @groupname com.bbim.bigdata
 * @create_date 2020-09-12
 * @description functionality and usage of class
 *
 */
package object streaming {

    lazy val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

}
