package flink.dataStream

import java.util.Collections

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import pojo.Person

/**
 * @author ：kenor
 * @date ：Created in 2020/8/29 18:35
 * @description：
 * @version: 1.0
 */
object FromJavaCollection extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val container = new java.util.LinkedList[Person]()
  Collections.addAll(container,
    new Person(1, "tom", "male"),
    new Person(2, "lisa", "female"))

  import org.apache.flink.api.scala._
  import scala.collection.JavaConversions._

  env.fromCollection(container).print()

  env.execute(this.getClass.getSimpleName)

}
