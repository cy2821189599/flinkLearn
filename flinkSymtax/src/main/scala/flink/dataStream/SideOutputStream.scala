package flink.dataStream

import flink.dataStream.SplitStream.Person
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


/**
 * @author ：kenor
 * @date ：Created in 2020/8/25 23:27
 * @description： optimization for SplitStream
 * @version: 1.0
 */
object SideOutputStream extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  import org.apache.flink.api.scala._
  //T->Person
  val outputTag = new OutputTag[Person]("younger")
  val mainStream = env.socketTextStream("localhost", 1111)
    .map(info => {
      val fields = info.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val height = fields(2).toInt
      val gender = fields(3)
      Person(name, age, height, gender)
    }).process(new ProcessFunction[Person, Person] {
    override def processElement(value: Person, ctx: ProcessFunction[Person, Person]#Context, out: Collector[Person]): Unit = {
      if (value.age >= 18) {
        out.collect(value)
      } else {
        //sideOutput
        ctx.output(outputTag, value)
      }
    }
  })
  mainStream.getSideOutput(outputTag).print("younger->")

  mainStream.print("adult->")

  env.execute()
}
