package com.tapdata.tm.config;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableMongoRepositories(mongoTemplateRef = "obsMongoTemplate", basePackages = {"com.tapdata.tm.monitor.service"})
public class ObsMongoConfig {

    @Value("${spring.data.mongodb.obs.uri}")
    private String obsUri;
    @Value("${spring.data.mongodb.default.uri}")
    private String defaultUri;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private List<String> productList;
    @Bean(name = "obsMongoTemplate")
    public MongoTemplate mongoTemplate() throws Exception {
        String uri = productList.contains("dfs") ? obsUri : defaultUri;
        MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(uri));
        MongoCollection<Document> agentMeasurementV2 = mongoTemplate.getCollection("AgentMeasurementV2");
        if (agentMeasurementV2.countDocuments() == 0) {
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
    }
}
