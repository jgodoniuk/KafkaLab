package com.qbology.kafka.consumer.simple;

import com.qbology.kafka.KafkaPropertiesLoader;
import com.qbology.kafka.consumer.ConsumerRecordsHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {

  private final Consumer<String, String> consumer;
  private final ConsumerRecordsHandler<String, String> recordsHandler;
  private boolean keepConsuming = true;

  public KafkaConsumerApp(final Consumer<String, String> consumer, final ConsumerRecordsHandler<String, String> recordsHandler) {
    this.consumer = consumer;
    this.recordsHandler = recordsHandler;
  }

  public void runConsume(final Properties props) {
    try (consumer) {
      consumer.subscribe(Collections.singletonList(props.getProperty("input.topic.name")));
      while (keepConsuming) {
        final var records = consumer.poll(Duration.ofSeconds(1));
        recordsHandler.process(records, "single");
      }
    }
  }

  public void shutdown() {
    keepConsuming = false;
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "This program takes four arguments: * the path to a configuration file, * username, * password");
    }

    final var props = KafkaPropertiesLoader.loadPropertiesWithPlaceholdersReplaced(args[0], args[1], args[2]);
    final var filePath = props.getProperty("file.path");
    final var recordsHandler = new FileWritingRecordsHandler(Path.of(filePath));

    try (final var consumer = new KafkaConsumer<String, String>(props)) {
      final var consumerApp = new KafkaConsumerApp(consumer, recordsHandler);

      Runtime.getRuntime().addShutdownHook(new Thread(consumerApp::shutdown));

      consumerApp.runConsume(props);
    }
  }
}
