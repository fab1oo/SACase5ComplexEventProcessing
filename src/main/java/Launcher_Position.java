import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

public class Launcher_Position {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-position" + Math.random());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.111.10:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("driver-position");
        KStream<String, String> return_stream =stream.map((key, value) -> {
            Utils.GpsPos gpsPos = Utils.extractCoordinates(value);

            Integer delay = Utils.requestDelay(key, gpsPos);
            String value_text = "id: " + key + ", delay: " + delay;

            System.out.println("Event Value: " + value_text);
            KeyValue return_value = KeyValue.pair(key, value_text);
            return return_value;
        });
        return_stream.to("group8-route-timing-final"); // For check if delay is significant

        /** For updating Dashboard http://192.168.111.11:8080/status/ **/
        //return_stream.to("delays");


        // instantiate our processing pipeline and run it
        final Topology topology = streamsBuilder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
