package com.qbology.kafka;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class KafkaProducerAppTest {

  private final static String TEST_CONFIG_FILE = "configuration/test.properties";

  @Test
  public void testProduce() throws IOException {
    final var stringSerializer = new StringSerializer();
    final var mockProducer = new MockProducer<>(true, stringSerializer, stringSerializer);
    final var props = KafkaPropertiesLoader.loadPropertiesWithPlaceholdersReplaced(TEST_CONFIG_FILE, username, password);
    final var topic = props.getProperty("output.topic.name");
    final var producerApp = new KafkaProducerApp(mockProducer, topic);
    final var records = Arrays.asList("foo-bar", "bar-foo", "baz-bar", "great:weather");

    records.forEach(producerApp::produce);

    final var expectedList = Arrays.asList(
        KeyValue.pair("foo", "bar"),
        KeyValue.pair("bar", "foo"),
        KeyValue.pair("baz", "bar"),
        KeyValue.pair("NO-KEY","great:weather"));

    final var actualList = mockProducer.history()
        .stream()
        .map(this::toKeyValue)
        .collect(Collectors.toList());

    MatcherAssert.assertThat(actualList, Matchers.equalTo(expectedList));
    producerApp.shutdown();
  }

  private KeyValue<String, String> toKeyValue(final ProducerRecord<String, String> producerRecord) {
    return KeyValue.pair(producerRecord.key(), producerRecord.value());
  }
}
