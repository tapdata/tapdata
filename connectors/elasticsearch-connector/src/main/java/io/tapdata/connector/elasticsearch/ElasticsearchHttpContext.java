package io.tapdata.connector.elasticsearch;

import io.tapdata.kit.EmptyKit;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;

import java.io.IOException;

public class ElasticsearchHttpContext {

    private static final String ELASTIC_SEARCH_SCHEME = "http";
    private static final int SOCKET_TIMEOUT_MILLIS = 1000000;
    private static final int CONNECT_TIMEOUT_MILLIS = 5000;
    private static final int CONNECTION_REQUEST_TIMEOUT_MILLIS = 50000;

    private final ElasticsearchConfig elasticsearchConfig;
    private final RestHighLevelClient elasticsearchClient;

    public ElasticsearchHttpContext(ElasticsearchConfig elasticsearchConfig) {
        this.elasticsearchConfig = elasticsearchConfig;
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        if (EmptyKit.isNotBlank(elasticsearchConfig.getUser()) && EmptyKit.isNotBlank(elasticsearchConfig.getPassword())) {
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticsearchConfig.getUser(), elasticsearchConfig.getPassword()));
        }
        HttpHost httpHost = new HttpHost(elasticsearchConfig.getHost(), elasticsearchConfig.getPort(), ELASTIC_SEARCH_SCHEME);
        elasticsearchClient = new RestHighLevelClient(RestClient.builder(httpHost)
                .setRequestConfigCallback(builder -> builder
                        .setSocketTimeout(SOCKET_TIMEOUT_MILLIS)
                        .setConnectTimeout(CONNECT_TIMEOUT_MILLIS)
                        .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MILLIS))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
                        .setDefaultCredentialsProvider(basicCredentialsProvider)));
    }

    public ElasticsearchConfig getElasticsearchConfig() {
        return elasticsearchConfig;
    }

    public RestHighLevelClient getElasticsearchClient() {
        return elasticsearchClient;
    }

    public String queryVersion() {
        try {
            return elasticsearchClient.info(RequestOptions.DEFAULT).getVersion().getNumber();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int countIndices() throws IOException {
        GetAliasesResponse getAliasesResponse = elasticsearchClient.indices().getAlias(new GetAliasesRequest(), RequestOptions.DEFAULT);
        return getAliasesResponse.getAliases().size();
    }

    public boolean existsIndex(String index) throws IOException {
        return elasticsearchClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
    }

    public void finish() throws IOException {
        elasticsearchClient.close();
    }

}
