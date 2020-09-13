package flink.dataSetApi

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

/**
 * @author ：kenor
 * @date ：Created in 2020/9/10 23:21
 * @description： boundedStream
 * @version: 1.0
 */
object BroadCast_join {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val ds1: DataSet[(Int, Char)] = env.fromElements((1, 'f'), (2, 'm'))

    val ds2: DataSet[(Int, String, Int, String)] = env.fromElements((102, "lisa", 2, "beijing"),
      (101, "peter", 1, "shanghai"), (103, "john", 2, "chengdu"))

    ds2.map(new RichMapFunction[(Int, String, Int, String), (String, Char, String)] {

      var map: mutable.Map[Int, Char] = mutable.Map()

      override def open(parameters: Configuration): Unit = {
        val list: util.List[(Int, Char)] = getRuntimeContext.getBroadcastVariable("dic")
        import scala.collection.JavaConversions._
        for (e <- list) {
          map.put(e._1, e._2)
        }
      }

      override def map(in: (Int, String, Int, String)): (String, Char, String) = {
        val gender = map.getOrElse(in._3, 'o')
        (in._2, gender, in._4)
      }

    }).withBroadcastSet(ds1, "dic")
      .collect()
      .foreach(println)

  }

}
