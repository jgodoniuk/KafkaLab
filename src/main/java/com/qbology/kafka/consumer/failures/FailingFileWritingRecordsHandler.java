package com.qbology.kafka.consumer.failures;

import com.qbology.kafka.consumer.ConsumerRecordsHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class FailingFileWritingRecordsHandler implements ConsumerRecordsHandler<String, String> {

  private final Path path;

  public FailingFileWritingRecordsHandler(Path path) {
    this.path = path;
  }

  @Override
  public void process(ConsumerRecords<String, String> records, String consumerId) {
    final var values = new ArrayList<String>();
    records.forEach(record -> values.add(describe(consumerId, record)));
    if (!values.isEmpty()) {
      values.forEach(value -> {
        try {
//          if (value.contains("bird")) {
//            throw new HandlingException("BIRD");
//          }
          Files.write(path, List.of(value), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      });
    }
  }

  private static String describe(String consumerId, ConsumerRecord<String, String> record) {
    return "value: %s, consumer: %s, partition: %s, offset: %s"
        .formatted(record.value(), consumerId, record.partition(), record.offset());
  }
}
