package flink.dataSetApi

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author ：kenor
 * @date ：Created in 2020/8/20 22:07
 * @description：
 * @version: 1.0
 */
object RollAggFunc {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val person: DataSet[Person] = env.readTextFile("e://temp/input/3.txt")
      .map(x => {
        val fields = x.split(",")
        val name = fields(0)
        val age = fields(1).toInt
        val height = fields(2).toInt
        val gender = fields(3)
        Person(name, age, height, gender)
      })
    person.groupBy("age").max("height").print()
  }

  case class Person(name: String, age: Int, height: Int, gender: String)

}
