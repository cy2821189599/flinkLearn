package flink.dataStream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author ：kenor
 * @date ：Created in 2020/8/27 23:33
 * @description：
 * @version: 1.0
 */
object FromElements extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  import org.apache.flink.api.scala._

  val scores: DataStream[Double] = env.fromElements(88.5, 93.5, 40, 60)
  scores.filter(_ >= 60).print()

  env.execute()
}
