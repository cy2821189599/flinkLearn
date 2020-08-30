package flink.dataStream

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author ：kenor
 * @date ：Created in 2020/8/30 23:00
 * @description：
 * @version: 1.0
 */

object UserDefineFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val socket = env.socketTextStream("localhost", 1111)
      .map(info => {
        val fields = info.split(",")
        val name = fields(0)
        val age = fields(1).toInt
        val height = fields(2).toInt
        val gender = fields(3)
        Person(name, age, height, gender)
      })
//      .filter(new FilterFunction[Person] {
//        override def filter(t: Person): Boolean = t.age > 17
//      })
      .filter(new UDFilter)
      .print("adult=>")

    env.execute(this.getClass.getSimpleName)
  }


  class UDFilter extends FilterFunction[Person] {
    override def filter(t: Person): Boolean = {
      t.age > 17
    }
  }

  case class Person(name: String, age: Int, height: Int, gender: String)

}
