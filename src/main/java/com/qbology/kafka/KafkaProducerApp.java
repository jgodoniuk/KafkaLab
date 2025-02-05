package com.qbology.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class KafkaProducerApp {
  private final Producer<String, String> producer;
  private final String outTopic;

  public KafkaProducerApp(Producer<String, String> producer, String outTopic) {
    this.producer = producer;
    this.outTopic = outTopic;
  }

  public Future<RecordMetadata> produce(final String message) {
    final String[] parts = message.split("-");
    final String key, value;
    if (parts.length > 1) {
      key = parts[0];
      value = parts[1];
    } else {
      key = "NO-KEY";
      value = parts[0];
    }
    final var producerRecord = new ProducerRecord<>(outTopic, key, value);
    return producer.send(producerRecord);
  }

  public void shutdown() {
    producer.close();
  }

  public void printMetadata(final Collection<Future<RecordMetadata>> metadata, final String fileName) {
    System.out.println("Offsets and timestamps committed in batch from " + fileName);
    metadata.forEach(m -> {
      try {
        final RecordMetadata recordMetadata = m.get();
        System.out.println("Record written to offset " + recordMetadata.offset() + " timestamp " + recordMetadata.timestamp());
      } catch (InterruptedException | ExecutionException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    });
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 4) {
      throw new IllegalArgumentException(
          "This program takes four arguments: 1. the path to a configuration file, 2. input file path 3. username, 4. password");
    }

    final var props = KafkaPropertiesLoader.loadPropertiesWithPlaceholdersReplaced(args[0], args[2], args[3]);
    final var outTopic = props.getProperty("topic.name");

    String inputFilePath = args[1];

    try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
      final var producerApp = new KafkaProducerApp(producer, outTopic);
      var linesToProduce = Files.readAllLines(Paths.get(inputFilePath));
      var metadata = linesToProduce.stream()
          .filter(l -> !l.trim().isEmpty())
          .map(producerApp::produce)
          .collect(Collectors.toList());
      producerApp.printMetadata(metadata, inputFilePath);

    } catch (IOException e) {
      System.err.printf("Error reading file %s due to %s %n", inputFilePath, e);
    }
  }
}
