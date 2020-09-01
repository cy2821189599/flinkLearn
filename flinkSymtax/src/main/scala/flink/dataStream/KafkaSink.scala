package flink.dataStream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
 * @author ：kenor
 * @date ：Created in 2020/9/1 22:34
 * @description：
 * @version: 1.0
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val consumerProps = new Properties()
    consumerProps.load(this.getClass.getClassLoader.getResourceAsStream("kafkaConsumer.properties"))
    //kafkaSource
    val src: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("my-topic",
      new SimpleStringSchema(), consumerProps))
      .map(e => {
        val fields = e.split(",")
        val name = fields(0)
        val age = fields(1).toInt
        val height = fields(2).toInt
        val gender = fields(3)
        Person(name, age, height, gender)
      }).map(_.toString)

    val producerProps = new Properties()
    producerProps.load(this.getClass.getClassLoader.getResourceAsStream("kafkaProducer.properties"))
    //kafkaSink
    src.addSink(new FlinkKafkaProducer[String]("target-topic", new SimpleStringSchema(), producerProps))

    env.execute("kafkaSinkDemo")
  }

  case class Person(name: String, age: Int, height: Int, gender: String)

}
