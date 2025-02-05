package com.qbology.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaPropertiesLoader {

  public static Properties loadPropertiesWithPlaceholdersReplaced(String propertiesFile, String username, String password) throws IOException {
    final var envProps = new Properties();
    final var input = new FileInputStream(propertiesFile);
    envProps.load(input);
    input.close();

    envProps.setProperty("sasl.jaas.config",
        envProps.getProperty("sasl.jaas.config")
            .replace("{USERNAME}", username)
            .replace("{PASSWORD}", password));

    return envProps;
  }

}
