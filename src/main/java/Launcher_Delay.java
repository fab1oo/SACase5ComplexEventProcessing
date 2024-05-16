import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Launcher_Delay {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-position" + Math.random());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.111.10:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("group8-route-timing-final");

        stream.foreach((key, value) -> {
            System.out.println("Incoming Event - " + value);

            Integer delay = Utils.extractDelay(value);

            if (delay > 180) {
                System.out.println("Id: " + key + ", Delay is greater than 3 minutes! Delay: " + delay);
            }
        });

        // instantiate our processing pipeline and run it
        final Topology topology = streamsBuilder.build();
        final KafkaStreams streams_delay = new KafkaStreams(topology, props);
        final CountDownLatch latch_delay = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams_delay.close();
                latch_delay.countDown();
            }
        });

        try {
            streams_delay.start();
            latch_delay.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
