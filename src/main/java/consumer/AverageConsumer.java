package consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class AverageConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, Long> source = builder.stream("random");
        
        KGroupedStream<String, Long> group = source.groupByKey();
        KTable<Windowed<String>, Long> sum = group.reduce((v1, v2) ->  v1 + v2, TimeWindows.of(60000L).advanceBy(60000L).until(60000L), "sum");
        KTable<Windowed<String>, Long> count = group.count(TimeWindows.of(60000L).advanceBy(60000L).until(60000L), "count");
        KTable<Windowed<String>, Long> avg = sum.join(count, (v1, v2) -> v1/v2);
        avg.toStream((k, v) -> k.key()).to("average");
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }
}
