package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.158.136:9092,192.168.158.137:9092,192.168.158.138:9092");
        properties.put("acks", "-1");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);//延迟发送时间
        properties.put("client.id", "ProducerTranscationnalExample");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("transactional.id", "test-transactional");
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<String, String>("my-topic", "key" + i, "message" + i));
                Thread.sleep(1000);
            }
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            e.printStackTrace();
        } catch (KafkaException e2) {
            e2.printStackTrace();
            producer.abortTransaction();
        }catch (InterruptedException e3) {
            e3.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
