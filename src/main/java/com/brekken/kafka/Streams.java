package com.brekken.kafka;

import com.brekken.kafka.model.IntermediateSumAndCount;
import com.brekken.kafka.model.ResultEvent;
import com.brekken.kafka.model.SourceEvent;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Streams {
    private static final Logger log = LoggerFactory.getLogger(Streams.class);
    private final StreamsBuilder streamsBuilder;
    private final Properties applicationProperties;

    public Streams(Properties applicationProperties) {
        this.applicationProperties = applicationProperties;
        streamsBuilder = new StreamsBuilder();
    }

    public void start() {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationProperties.get("application.id"));
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, applicationProperties.get("bootstrap.servers"));
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        Topology topology = buildRunningAverageTopology();
        try(KafkaStreams streams = new KafkaStreams(topology, streamsProps)) {
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

    Topology buildRunningAverageTopology() {
        Serde<Windowed<String>> windowedKeySerde = Serdes.serdeFrom(new TimeWindowedSerializer<>(new StringSerializer()),
                new TimeWindowedDeserializer<>(new StringDeserializer(), Duration.ofMinutes(5).toMillis()));
        //consume source topic and produce running averages for certain fields
        streamsBuilder.stream(applicationProperties.getProperty("input.topic"), Consumed.with(Serdes.String(), getJsonValueSerde(SourceEvent.class)))
                .peek((key, value) -> {
                    log.info("key is {}, value is {}", key, value);
                })
                // filter out any messages that have a type of 'XXX'
                .filter((key, value) -> !value.getType().equalsIgnoreCase("XXX"))
                //re-key the stream of data by the id
                .selectKey((key, value) -> value.getId())
                .peek((key, value) -> {
                    log.info("after re-key, key is {}, value is {}", key, value);
                })
                .groupByKey()
                // group the data into 5 minute windows
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(30)))
                .aggregate(IntermediateSumAndCount::new, (key, value, aggregate) -> {
                    aggregate.setCount(aggregate.getCount() + 1);
                    aggregate.setSpeedSum(aggregate.getSpeedSum() + value.getSpeed());
                    aggregate.setPriceSum(aggregate.getPriceSum() + value.getPrice());
                    //now compute averages
                    aggregate.setAverageSpeed(aggregate.getSpeedSum() / aggregate.getCount());
                    aggregate.setAveragePrice(aggregate.getPriceSum() / aggregate.getCount());

                    return aggregate;
                }, Materialized.with(Serdes.String(), getJsonValueSerde(IntermediateSumAndCount.class)))
                .mapValues((readOnlyKey, value) -> {
                    ResultEvent result = new ResultEvent();
                    result.setId(readOnlyKey.key());
                    result.setAverageSpeed(value.getAverageSpeed());
                    result.setAveragePrice(value.getAveragePrice());
                    return result;
                })
                .toStream()
                .to(applicationProperties.getProperty("sink.topic"), Produced.with(windowedKeySerde, getJsonValueSerde(ResultEvent.class)));

        return streamsBuilder.build();
    }

    static <T> Serde<T> getJsonValueSerde(Class<T> c) {
        Serde<T> jsonSerde = Serdes.serdeFrom(new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>());
        final Map<String, String> serdeConfig = Collections.singletonMap(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, c.getName());
        jsonSerde.configure(serdeConfig, false);

        return jsonSerde;
    }
}
