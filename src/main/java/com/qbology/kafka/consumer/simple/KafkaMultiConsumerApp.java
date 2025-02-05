package com.qbology.kafka.consumer.simple;

import com.qbology.kafka.KafkaPropertiesLoader;
import com.qbology.kafka.consumer.KafkaConsumerLoop;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.Executors;

public class KafkaMultiConsumerApp {

  public static void main(String[] args) throws IOException {
    if (args.length < 4) {
      throw new IllegalArgumentException(
          "This program takes four arguments: 1. the path to a configuration file, 2. number of consumers to run, 3. username, 4. password");
    }
    var consumerCount = Integer.parseInt(args[1]);

    var props = KafkaPropertiesLoader.loadPropertiesWithPlaceholdersReplaced(args[0], args[2], args[3]);
    var filePath = props.getProperty("file.path");
    var topic = props.getProperty("input.topic.name");

//    runTimeBoundLoop(consumerCount, props, filePath, topic);
    runWakeupLoop(consumerCount, props, filePath, topic);
  }

  private static void runTimeBoundLoop(int consumerCount, Properties props, String filePath, String topic) {
    var executorService = Executors.newFixedThreadPool(consumerCount);

    for (int i = 0; i < consumerCount; i++) {
      var id = "id-" + i;
      props.setProperty("client.id", props.getProperty("client.id") + "-" + i);
      var consumer = new KafkaConsumer<String, String>(props);
      var recordsHandler = new FileWritingRecordsHandler(Path.of(filePath));
      var consumerLoop = new KafkaConsumerLoop(id, consumer, recordsHandler, topic);

      executorService.submit(consumerLoop);

      Runtime.getRuntime().addShutdownHook(new Thread(consumerLoop::shutdown));
    }
  }

  private static void runWakeupLoop(int consumerCount, Properties props, String filePath, String topic) {
    var executorService = Executors.newFixedThreadPool(consumerCount);

    for (int i = 0; i < consumerCount; i++) {
      var id = "id-" + i;
      props.setProperty("client.id", "client-id-" + i);
      var consumer = new KafkaConsumer<String, String>(props);
      var recordsHandler = new FileWritingRecordsHandler(Path.of(filePath));
      var consumerLoop = new KafkaConsumerWithWakeupLoop(id, consumer, recordsHandler, topic);

      executorService.submit(consumerLoop);

      Runtime.getRuntime().addShutdownHook(new Thread(consumerLoop::shutdown));
    }
  }
}
