package flink.dataStream

import flink.dataStream.KafkaSink.Person
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author ：kenor
 * @date ：Created in 2020/9/21 23:53
 * @description：
 * @version: 1.0
 */
object AggSum {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.socketTextStream("localhost", 9999)
      .filter(_.nonEmpty)
      .map(info => {
        val fields = info.split(",")
        val name = fields(0).trim
        val age = fields(1).trim.toInt
        val height = fields(2).trim.toInt
        val gender = fields(3).trim
        Person(name, age, height, gender)
      })
      .map(p => {
        if (p.age > 17) {
          ("adult", 1)
        } else {
          ("young", 1)
        }
      }).keyBy(0)
      .sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)
  }

}
