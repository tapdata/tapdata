package io.tapdata.connector.kafka.admin;

import io.tapdata.connector.kafka.config.AdminConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Set;

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
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
