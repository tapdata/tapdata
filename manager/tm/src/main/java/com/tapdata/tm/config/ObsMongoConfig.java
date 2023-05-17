package com.tapdata.tm.config;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.mongo.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.*;
import org.springframework.data.mongodb.core.MongoClientFactoryBean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableAsync
@EnableMongoRepositories(mongoTemplateRef = "obsMongoTemplate", basePackages = {"com.tapdata.tm.monitor.service"})
public class ObsMongoConfig implements AsyncConfigurer {

    @Value("${spring.data.mongodb.obs.uri}")
    private String obsUri;
    @Value("${spring.data.mongodb.default.uri}")
    private String defaultUri;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private List<String> productList;

    @Bean(name = "obsMongoTemplate")
    public CompletableFuture<MongoTemplate> mongoTemplate() throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String uri = productList.contains("dfs") ? obsUri : defaultUri;
                MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(uri));
                MongoCollection<Document> agentMeasurementV2 = mongoTemplate.getCollection("AgentMeasurementV2");
                if (agentMeasurementV2.estimatedDocumentCount() == 0) {
                    agentMeasurementV2.dropIndexes();

                    agentMeasurementV2.createIndex(Indexes.compoundIndex(new BsonDocument("grnty", new BsonInt32(1)),
                            new BsonDocument("tags.taskId", new BsonInt32(1)),
                            new BsonDocument("tags.type", new BsonInt32(1)),
                            new BsonDocument("tags.taskIdRecordId", new BsonInt32(1)),
                            new BsonDocument("tags.table", new BsonInt32(1))));

                    agentMeasurementV2.createIndex(Indexes.compoundIndex(new BsonDocument("grnty", new BsonInt32(1)),
                            new BsonDocument("tags.taskId", new BsonInt32(1)),
                            new BsonDocument("tags.type", new BsonInt32(1)),
                            new BsonDocument("tags.taskIdRecordId", new BsonInt32(1)),
                            new BsonDocument("tags.nodeId", new BsonInt32(1))));

                    agentMeasurementV2.createIndex(Indexes.compoundIndex(new BsonDocument("grnty", new BsonInt32(1)),
                            new BsonDocument("tags.engineId", new BsonInt32(1)),
                            new BsonDocument("tags.type", new BsonInt32(1))));

                    agentMeasurementV2.createIndex(new BsonDocument("date", new BsonInt32(-1)), new IndexOptions().expireAfter(7L, TimeUnit.DAYS));
                }

                return mongoTemplate;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
