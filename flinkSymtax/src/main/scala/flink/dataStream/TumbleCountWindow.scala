package flink.dataStream

import flink.dataStream.KafkaSink.Person
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ：kenor
 * @date ：Created in 2020/10/9 23:06
 * @description：
 * @version: 1.0
 */
object TumbleCountWindow {
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
      }).keyBy(0)
      .countWindow(3)
      .reduce((now, old) => {
        if (now.height > old.height) {
          now
        } else {
          old
        }
      }).print("=>")

    env.execute(this.getClass.getSimpleName)
  }

}
