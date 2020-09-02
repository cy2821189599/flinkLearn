package flink.dataStream

import java.lang
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

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
      .filter(_.nonEmpty)
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
    // src.addSink(new FlinkKafkaProducer[String]("target-topic", new SimpleStringSchema(), producerProps))
    val target_topic = "target-topic"

    // optimize serialization
    val serializationSchema: KafkaSerializationSchema[String] = new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]](target_topic, element.getBytes(StandardCharsets.UTF_8))
      }
    }

    val semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE

    src.addSink(new FlinkKafkaProducer[String](target_topic, serializationSchema, producerProps, semantic))

    env.execute("kafkaSinkDemo")
  }

  case class Person(name: String, age: Int, height: Int, gender: String)

}
