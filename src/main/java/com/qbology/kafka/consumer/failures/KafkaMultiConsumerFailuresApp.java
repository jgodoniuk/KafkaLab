package com.qbology.kafka.consumer.failures;

import com.qbology.kafka.KafkaPropertiesLoader;
import com.qbology.kafka.consumer.simple.KafkaConsumerWithWakeupLoop;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.Executors;

public class KafkaMultiConsumerFailuresApp {

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      throw new IllegalArgumentException(
          "This program takes four arguments: * the path to a configuration file, * number of consumers to run, * username, * password");
    }
    var consumerCount = Integer.parseInt(args[1]);

    var props = KafkaPropertiesLoader.loadPropertiesWithPlaceholdersReplaced(args[0], args[2], args[3]);
    var filePath = props.getProperty("file.path");
    var topic = props.getProperty("input.topic.name");

    runLoopWithFailingHandler(consumerCount, props, filePath, topic);
  }

  private static void runLoopWithFailingHandler(int consumerCount, Properties props, String filePath, String topic) {
    var executorService = Executors.newFixedThreadPool(consumerCount);

    for (int i = 0; i < consumerCount; i++) {
      var id = "id-" + i;
      props.setProperty("client.id", "client-id-" + i);
      var consumer = new KafkaConsumer<String, String>(props);
      var recordsHandler = new FailingFileWritingRecordsHandler(Path.of(filePath));
      var consumerLoop = new KafkaConsumerWithSyncCommitLoop(id, consumer, recordsHandler, topic);

      executorService.submit(consumerLoop);

      Runtime.getRuntime().addShutdownHook(new Thread(consumerLoop::shutdown));
    }
  }
}
