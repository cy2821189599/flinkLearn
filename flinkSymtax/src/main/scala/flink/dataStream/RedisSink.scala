package flink.dataStream

import java.net.InetSocketAddress

import flink.dataStream.KafkaSink.Person
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisConfigBase}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
 * @author ：kenor
 * @date ：Created in 2020/9/3 23:18
 * @description：
 * @version: 1.0
 */

object RedisSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val result = env.socketTextStream("localhost", 8888)
      .filter(_.nonEmpty)
      .map(info => {
        val fields = info.split(",")
        val name = fields(0)
        val age = fields(1).toInt
        val height = fields(2).toInt
        val gender = fields(3)
        Person(name, age, height, gender)
      }).filter(_.age < 18)
    val nodes:Set[InetSocketAddress] = Set(
       new InetSocketAddress("192.168.158.136",7005)
      ,new InetSocketAddress("192.168.158.136",7006)
      ,new InetSocketAddress("192.168.158.137",7003)
      ,new InetSocketAddress("192.168.158.137",7004)
      ,new InetSocketAddress("192.168.158.138",7001)
      ,new InetSocketAddress("192.168.158.138",7002)
    )
    import scala.collection.JavaConversions._
    val config:FlinkJedisConfigBase = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build()

    val mapper:RedisMapper[Person] = new MyRedisMapper

    result.addSink(new RedisSink(config, mapper))

    env.execute()
  }
}

class MyRedisMapper extends RedisMapper[Person]{
  // define data type to store in redis
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"persons")
  }

  // define key
  override def getKeyFromData(t: Person): String = {
    t.name
  }
  // define value
  override def getValueFromData(t: Person): String = {
    t.toString
  }
}