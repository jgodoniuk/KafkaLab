package com.qbology.kafka.consumer.failures;

import com.qbology.kafka.consumer.ConsumerRecordsHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;

public class KafkaConsumerWithSyncCommitLoop implements Runnable {

  private final Consumer<String, String> consumer;
  private final ConsumerRecordsHandler<String, String> recordsHandler;
  private final String id;
  private final String topic;

  public KafkaConsumerWithSyncCommitLoop(String id,
                                         Consumer<String, String> consumer,
                                         ConsumerRecordsHandler<String, String> recordsHandler,
                                         String topic) {
    this.consumer = consumer;
    this.recordsHandler = recordsHandler;
    this.id = id;
    this.topic = topic;
  }

  public void shutdown() {
    consumer.wakeup();
  }

  @Override
  public void run() {
    System.out.println("STARTING WAKEUP SYNC COMMIT CONSUMER ID: " + id);
    consumer.subscribe(Collections.singletonList(topic));
    try {
      while (true) {
        try {
          System.out.println("NEXT POLL");
          final var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE)); // Block indefinitely until the next records can be returned
          recordsHandler.process(records, id);
          consumer.commitSync();
        } catch (HandlingException e) {
          System.out.println(e);
        }
      }
    } catch (WakeupException e) {
      // Ignore for shutdown
      System.out.println("WAKEUP EXCEPTION");
    } finally {
      System.out.println("CONSUMER CLOSING");
      consumer.close();
    }
  }
}
