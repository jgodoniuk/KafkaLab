package com.qbology.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.Collections;

public class KafkaConsumerLoop implements Runnable {

  private final Consumer<String, String> consumer;
  private final ConsumerRecordsHandler<String, String> recordsHandler;
  private final String id;
  private final String topic;
  private boolean keepConsuming = true;

  public KafkaConsumerLoop(String id,
                           Consumer<String, String> consumer,
                           ConsumerRecordsHandler<String, String> recordsHandler,
                           String topic) {
    this.consumer = consumer;
    this.recordsHandler = recordsHandler;
    this.id = id;
    this.topic = topic;
  }

  public void shutdown() {
    keepConsuming = false;
  }

  @Override
  public void run() {
    System.out.println("STARTING CONSUMER ID: " + id);
    consumer.subscribe(Collections.singletonList(topic));
    while (keepConsuming) {
      final var records = consumer.poll(Duration.ofSeconds(1));
      recordsHandler.process(records, id);
    }
  }
}
