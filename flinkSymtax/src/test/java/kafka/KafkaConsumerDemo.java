package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.158.136:9092,192.168.158.137:9092,192.168.158.138:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "false");//设置为手动提交offset
//        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", consumerRecord.partition(),
                        consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
            }
            kafkaConsumer.commitAsync();//提交offset
        }
    }
}
