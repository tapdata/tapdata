package io.tapdata.connector.kafka.admin;

import io.tapdata.connector.kafka.config.AdminConfiguration;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DefaultAdmin implements Admin {

    private final AdminClient adminClient;

    public DefaultAdmin(AdminConfiguration configuration) {
        this.adminClient = AdminClient.create(configuration.build());
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    @Override
    public boolean isClusterConnectable() {
        try {
            return adminClient.describeCluster().controller().get() != null;
        } catch (Throwable t) {
            throw new RuntimeException("fetch cluster controller error, " + t.getMessage(), t);
        }
    }

    @Override
    public Set<String> listTopics() {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        try {
            return adminClient.listTopics(options).names().get();
        } catch (Throwable t) {
            throw new RuntimeException("fetch topic list error", t);
        }
    }

    @Override
    public void createTopics(Set<String> topics) {
        Set<NewTopic> newTopics = topics.stream()
                .map(topic -> new NewTopic(topic, 3, (short) 1))
                .collect(java.util.stream.Collectors.toSet());
        adminClient.createTopics(newTopics);
    }

    @Override
    public void createTopics(String topic, int numPartitions, short replicationFactor) {
        NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
        adminClient.createTopics(Collections.singleton(newTopic));
    }

    @Override
    public void addTopicPartitions(String topic,Integer numPartitions) {
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topic, newPartitions);
        CreatePartitionsResult partitions = adminClient.createPartitions(newPartitionsMap);
    }

    public List<TopicPartitionInfo> getTopicPartitionInfo(String topic){
        List<String> list=new ArrayList<>();
        list.add(topic);
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(list);
        try {
            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.allTopicNames().get();
            TopicDescription topicDescription = stringTopicDescriptionMap.get(topic);
            List<TopicPartitionInfo> partitions = topicDescription.partitions();
            return partitions;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
