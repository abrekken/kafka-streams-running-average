package com.brekken.kafka;

import com.brekken.kafka.model.ResultEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class StreamsTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<Windowed<String>, ResultEvent> outputTopic;
    private final Serde<String> stringSerde = new Serdes.StringSerde();

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @BeforeEach
    public void setup() {
        Properties testProperties = App.loadProperties("application-test.properties");
        testProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        testProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        Streams streams = new Streams(testProperties);
        Topology topology = streams.buildRunningAverageTopology();
        testDriver = new TopologyTestDriver(topology, testProperties);
        inputTopic = testDriver.createInputTopic("source", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("output",
                new TimeWindowedDeserializer<>(new StringDeserializer(), Duration.ofMinutes(5).toMillis()), Streams.getJsonValueSerde(ResultEvent.class).deserializer());
    }

    @Test
    public void TestAveragesAreCalculatedCorrectly() {
        //message 1:
        inputTopic.pipeInput(null, getMessage1());

        KeyValue<Windowed<String>, ResultEvent> resultMsg1 = outputTopic.readKeyValue();
        assertThat(resultMsg1.key.key()).isEqualTo("ABC1");
        assertThat(resultMsg1.value.getAverageSpeed()).isEqualTo(25.0);
        assertThat(resultMsg1.value.getAveragePrice()).isEqualTo(38.4);

        //message 2:
        inputTopic.pipeInput(null, getMessage2());

        KeyValue<Windowed<String>, ResultEvent> resultMsg2 = outputTopic.readKeyValue();
        assertThat(resultMsg2.key.key()).isEqualTo("ABC1");
        assertThat(resultMsg2.value.getAverageSpeed()).isEqualTo(40);
        assertThat(resultMsg2.value.getAveragePrice()).isEqualTo(50.9);

        //message 3:
        inputTopic.pipeInput(null, getMessage3());

        KeyValue<Windowed<String>, ResultEvent> resultMsg3 = outputTopic.readKeyValue();
        assertThat(resultMsg3.key.key()).isEqualTo("ABC1");
        assertThat(resultMsg3.value.getAverageSpeed()).isEqualTo(36.33);
        assertThat(resultMsg3.value.getAveragePrice()).isEqualTo(52.43);

    }

    private String getMessage1() {
        return "{\n" +
                "  \"id\": \"ABC1\",\n" +
                "  \"type\": \"G-44\",\n" +
                "  \"speed\": \"25.0\",\n" +
                "  \"price\": \"38.4\"\n" +
                "}";
    }

    private String getMessage2() {
        return "{\n" +
                "  \"id\": \"ABC1\",\n" +
                "  \"type\": \"G-44\",\n" +
                "  \"speed\": \"55.0\",\n" +
                "  \"price\": \"63.4\"\n" +
                "}";
    }

    private String getMessage3() {
        return "{\n" +
                "  \"id\": \"ABC1\",\n" +
                "  \"type\": \"G-44\",\n" +
                "  \"speed\": \"29.0\",\n" +
                "  \"price\": \"55.5\"\n" +
                "}";
    }
}
