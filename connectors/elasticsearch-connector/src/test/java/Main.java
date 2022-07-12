import io.tapdata.connector.elasticsearch.ElasticsearchConfig;
import io.tapdata.connector.elasticsearch.ElasticsearchHttpContext;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        ElasticsearchConfig config = new ElasticsearchConfig();
        config.setHost("192.168.1.189");
        config.setPort(9204);
        ElasticsearchHttpContext context = new ElasticsearchHttpContext(config);
        context.getElasticsearchClient().info(RequestOptions.DEFAULT).getVersion().getNumber();
        context.finish();
    }
}
