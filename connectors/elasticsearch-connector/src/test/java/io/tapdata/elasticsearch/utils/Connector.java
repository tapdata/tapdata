package io.tapdata.elasticsearch.utils;

import io.tapdata.connector.elasticsearch.ElasticsearchConfig;
import io.tapdata.connector.elasticsearch.ElasticsearchHttpContext;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/28 14:02 Create
 */
public interface Connector {

    static void connect(ElasticsearchConfig config, Consumer<RestHighLevelClient> consumer) throws IOException {
        ElasticsearchHttpContext context = new ElasticsearchHttpContext(config);
        try (RestHighLevelClient client = context.getElasticsearchClient()) {
            consumer.accept(client);
        } finally {
            context.finish();
        }
    }

    static void connect(Map<String, Object> conf, Consumer<RestHighLevelClient> consumer) throws IOException {
        ElasticsearchConfig config = new ElasticsearchConfig().load(conf);
        connect(config, consumer);
    }

    static void connect(String host, int port, Consumer<RestHighLevelClient> consumer) throws IOException {
        ElasticsearchConfig config = new ElasticsearchConfig();
        config.setHost(host);
        config.setPort(port);
        connect(config, consumer);
    }

    static void connect(String host, int port, String user, String pass, Consumer<RestHighLevelClient> consumer) throws IOException {
        ElasticsearchConfig config = new ElasticsearchConfig();
        config.setHost(host);
        config.setPort(port);
        config.setUser(user);
        config.setPassword(pass);
        connect(config, consumer);
    }

    static String versionNumber(RestHighLevelClient client) throws IOException {
        return client.info(RequestOptions.DEFAULT).getVersion().getNumber();
    }
}
