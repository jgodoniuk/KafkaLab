package com.qbology.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ConsumerRecordsHandler<K, V> {

  void process(ConsumerRecords<K, V> records, String consumerId);
}
