package flink.dataStream

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

/**
 * @author ：kenor
 * @date ：Created in 2020/9/15 21:40
 * @description：
 * @version: 1.0
 */
object DistributeCache {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.registerCachedFile("hdfs://ns1/flink/cache/genderInfo.txt", "genderInfo")

    import org.apache.flink.api.scala._
    env.socketTextStream("localhost", 9999)
      .filter(_.nonEmpty)
      .map(new RichMapFunction[String, (Int, String, Char, String)] {
        var genderMap: mutable.Map[Int, Char] = mutable.HashMap()

        var bufferedSource:BufferedSource = _

        override def open(parameters: Configuration): Unit = {
          // get distribute Cache object
          val genderInfo: File = getRuntimeContext.getDistributedCache.getFile("genderInfo")
          bufferedSource = Source.fromFile(genderInfo)
          val lst = bufferedSource.getLines().toList
          for (elem <- lst) {
            val arr = elem.split(",")
            val genderId = arr(0).trim.toInt
            val gender = arr(1).trim.toCharArray()(0)
            genderMap.put(genderId, gender)
          }
        }

        override def map(in: String): (Int, String, Char, String) = {
          val fields = in.split(",")
          val id = fields(0).trim.toInt
          val name = fields(1).trim
          val genderFlag = genderMap.getOrElse(fields(2).trim.toInt, 'o')
          val address = fields(3).trim
          (id, name, genderFlag, address)
        }

        override def close(): Unit = {
          if(bufferedSource != null){
            bufferedSource.close()
          }
        }
      }).print()

    env.execute(this.getClass.getSimpleName)
  }

}
