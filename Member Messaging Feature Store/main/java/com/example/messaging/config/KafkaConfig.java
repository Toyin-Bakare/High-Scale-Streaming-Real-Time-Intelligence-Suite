package com.example.messaging.config;

import java.util.Properties;

public class KafkaConfig {
  public static Properties base(String bootstrapServers, String groupId) {
    Properties p = new Properties();
    p.setProperty("bootstrap.servers", bootstrapServers);
    p.setProperty("group.id", groupId);
    p.setProperty("auto.offset.reset", "latest");
    p.setProperty("enable.auto.commit", "false");
    // For MSK IAM auth, you'd add SASL/IAM configs here (see README section).
    return p;
  }
}
