package flink.dataSetApi

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.io.Source

/**
 * @author ：kenor
 * @date ：Created in 2020/8/15 20:55
 * @description：
 * @version: 1.0
 */
object BufferSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val list = Source.fromFile("e://temp/input/3.txt", "utf-8").getLines().toList
    import org.apache.flink.api.scala._
    val dataStream: DataStream[String] = env.fromCollection(list)
    dataStream.map(e => {
      val arr = e.split(",")
      val name = arr(0)
      val age = arr(1).toInt
      val height = arr(2).toInt
      val gender = arr(3)
      Person(name, age, height, gender)
    }).print
//    env.execute(this.getClass.getSimpleName)
  }

  case class Person(name: String, age: Int, height: Int, gender: String)

}
