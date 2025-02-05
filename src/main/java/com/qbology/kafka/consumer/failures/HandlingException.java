package com.qbology.kafka.consumer.failures;

public class HandlingException extends RuntimeException {
  public HandlingException(String message) {
    super(message);
  }
}
