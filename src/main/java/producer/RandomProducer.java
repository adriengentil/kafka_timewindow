package producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Random;
public class RandomProducer {
    public static void main(String[] args) {
        Properties props1 = new Properties();
        props1.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
        props1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        KafkaProducer<String, Long> producer = new KafkaProducer<>(props1);
        
        try {
            Random randomGenerator = new Random();
            int i = 10;
        	while (true)
        	{
        		producer.send(new ProducerRecord<>("random", "A", Long.valueOf(randomGenerator.nextInt(100)))).get();
          	}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        producer.close();
    }
}