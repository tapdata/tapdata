package io.tapdata.connector.kafka.admin;

import java.util.Set;

public interface Admin extends AutoCloseable {

  boolean isClusterConnectable();

  Set<String> listTopics();

  void createTopics(Set<String> topics);

}
