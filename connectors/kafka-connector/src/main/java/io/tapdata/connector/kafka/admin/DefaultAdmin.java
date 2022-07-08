package io.tapdata.connector.kafka.admin;

import io.tapdata.connector.kafka.config.AdminConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;

import java.util.Set;

public class DefaultAdmin implements Admin {

    private final AdminClient adminClient;

    public DefaultAdmin(AdminConfiguration configuration) {
        this.adminClient = AdminClient.create(configuration.build());
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
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
