package flink.dataStream

import java.util.{Collections, Properties}

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
    new Person(2, "root", "female"),
    new Person(3, "lisa", "female"))

  import org.apache.flink.api.scala._
  import scala.collection.JavaConversions._

  val props = new Properties()
  props.load(this.getClass.getClassLoader.getResourceAsStream("kafkaProducer.properties"))
  env.fromCollection(container)
    .flatMap(new MyRichFunction("my-topic", props))
    .print("general user=>")

  env.execute(this.getClass.getSimpleName)


}
