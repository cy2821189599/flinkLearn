package flink.dataStream

import java.util.Properties

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import pojo.Person

/**
 * @author ：kenor
 * @date ：Created in 2020/8/31 21:48
 * @description：
 * @version: 1.0
 */

/**
 *
 * @param topic kafka topic
 * @param prop  kafka config
 */
class MyRichFunction(topic: String, prop: Properties) extends RichFlatMapFunction[Person, Person] {

  private var producer: KafkaProducer[String, String] = _

  //only initialize once on open the dataStream
  override def open(parameters: Configuration): Unit = {
    producer = new KafkaProducer[String, String](prop)
  }

  //called on processing every element of the dataStream
  override def flatMap(in: Person, collector: Collector[Person]): Unit = {
    if (in == null || "root".equals(in.getName)) {
      producer.send(new ProducerRecord[String, String](topic, in.getId.toString, in.toString))
    } else {
      collector.collect(in)
    }
  }

  override def close(): Unit = {
    if (producer != null) {
      producer.close()
    }
  }
}
