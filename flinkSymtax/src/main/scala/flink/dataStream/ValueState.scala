package flink.dataStream

import flink.dataStream.KafkaSink.Person
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author ：kenor
 * @date ：Created in 2020/9/21 21:20
 * @description：
 * @version: 1.0
 */
object ValueState {
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
      .keyBy("name")
      .flatMapWithState[(Person, String), Float] {
        case (person, None) => {
          val height = person.height
          val isStandard = height >= 175 && height <= 180
          if (isStandard) {
            (List.empty, Some(person.height))
          } else {
            (List((person, s"${person.name}, your height is not standard,please be care of yourself")), Some(person.height))
          }
        }
        case (person, lstValue) => {
          val height = person.height
          val lstHeight = lstValue.get
          val isStandard = height >= 175 && height <= 180
          if (isStandard) {
            val diff = (height - lstHeight).abs
            if (diff > 10) {
              (List((person, s"${person.name},your height change big,over the range of threshold")), Some(person.height))
            } else {
              (List.empty, Some(person.height))
            }
          } else {
            (List((person, s"${person.name},your height is not standard,please be care of yourself")), Some(person.height))
          }
        }
      }
        .print()


    env.execute(this.getClass.getSimpleName)
  }

}
