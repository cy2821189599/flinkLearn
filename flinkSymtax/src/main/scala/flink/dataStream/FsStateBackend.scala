package flink.dataStream

import java.util.concurrent.TimeUnit

import flink.dataStream.KafkaSink.Person
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author ：kenor
 * @date ：Created in 2020/9/22 22:47
 * @description：
 * @version: 1.0
 */

object FsStateBackend {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //suggest config in the config file
    env.setStateBackend(new FsStateBackend("hdfs://ns1/flink/state/fs"))

    env.enableCheckpointing(5000)

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
      .map(p => {
        if (p.age > 17) {
          ("adult", 1)
        } else {
          ("young", 1)
        }
      }).keyBy(0)
      .sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,
      Time.of(10, TimeUnit.SECONDS)))
  }

}
