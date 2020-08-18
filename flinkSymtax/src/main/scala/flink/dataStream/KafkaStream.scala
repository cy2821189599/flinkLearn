package flink.dataStream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author ：kenor
 * @date ：Created in 2020/8/17 21:38
 * @description：
 * @version: 1.0
 */
object KafkaStream extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  import org.apache.flink.api.scala._

  val props = new Properties()
  props.load(this.getClass.getClassLoader.getResourceAsStream("kafkaConsumer.properties"))
  env.addSource(new FlinkKafkaConsumer[String]("my-topic", new SimpleStringSchema(), props))
    .print("kafka->")
  env.execute(this.getClass.getSimpleName)
}
