package flink.dataStream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author ：kenor
 * @date ：Created in 2020/8/25 22:42
 * @description：
 * @version: 1.0
 */
object SplitStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val splitStream = env.socketTextStream("localhost", 1111)
      .map(info => {
        val fields = info.split(",")
        val name = fields(0)
        val age = fields(1).toInt
        val height = fields(2).toInt
        val gender = fields(3)
        Person(name, age, height, gender)
      }).split((person) => {
      if (person.age >= 18) Seq("Adult") else Seq("young")
    })

    splitStream.select("Adult").print("Adult->")

    splitStream.select("young").print("younger->")
    /*
    Adult->:1> Person(tom2,19,167,男)
    Adult->:2> Person(tom12,19,169,男)
    Adult->:3> Person(tom12,19,169,男)
    younger->:4> Person(tom12,15,169,男)
    younger->:1> Person(tom12,15,169,男)
    younger->:2> Person(tom12,15,169,男)
    Adult->:3> Person(tom7,24,177,女)
    Adult->:1> Person(tom10,23,165,女)
    Adult->:2> Person(tom11,18,163,女)
    Adult->:4> Person(tom8,19,197,男)
    younger->:3> Person(tom12,15,169,男)
    */
    env.execute()
  }

  case class Person(name: String, age: Int, height: Int, gender: String)

}
