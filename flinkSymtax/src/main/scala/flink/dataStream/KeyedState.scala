package flink.dataStream

import flink.dataStream.KafkaSink.Person
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author ：kenor
 * @date ：Created in 2020/9/20 22:14
 * @description：
 * @version: 1.0
 */
object KeyedState {
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
      .flatMap(new MyFlatMapRichFunction(10))
      .print()

    env.execute(this.getClass.getSimpleName)
  }

  class MyFlatMapRichFunction(threshold: Double) extends RichFlatMapFunction[Person, (Person, String)] {
    var tmpValueState: ValueState[Float] = _

    //register
    override def open(parameters: Configuration): Unit = {
      val desc: ValueStateDescriptor[Float] = new ValueStateDescriptor[Float]("age", classOf[Float])
      tmpValueState = getRuntimeContext.getState[Float](desc)
    }

    override def flatMap(in: Person, collector: Collector[(Person, String)]): Unit = {
      val height = in.height
      val lastHeight = tmpValueState.value()
      val isStandard = height >= 175 && height <= 180
      if (isStandard) {
        if (lastHeight > 0.0) {
          val diff = (height - lastHeight).abs
          if (diff > threshold) {
            collector.collect(in, "your height change big,over the range of threshold")
          }
        }
      } else {
        collector.collect(in, "you height is not standard,please be care of yourself")
      }
      tmpValueState.update(height)
    }
  }


}
