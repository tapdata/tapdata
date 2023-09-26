package io.tapdata.connector.kafka.admin;

import org.apache.kafka.common.TopicPartitionInfo;

import java.util.List;
import java.util.Set;

public interface Admin extends AutoCloseable {

  boolean isClusterConnectable();

  Set<String> listTopics();

  void createTopics(Set<String> topics);

  void createTopics(String topic, int numPartitions, short replicationFactor);

  void addTopicPartitions(String topic,Integer numPartitions);

  List<TopicPartitionInfo> getTopicPartitionInfo(String topic);
}
