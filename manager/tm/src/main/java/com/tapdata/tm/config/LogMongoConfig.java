package com.tapdata.tm.config;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.tapdata.tm.utils.Lists;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableMongoRepositories(mongoTemplateRef = "logMongoTemplate", basePackages = {"com.tapdata.tm.monitoringlogs.service"})
public class LogMongoConfig {

    @Value("${spring.data.mongodb.log.uri}")
    private String logUri;
    @Value("${spring.data.mongodb.default.uri}")
    private String defaultUri;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private List<String> productList;

    @Bean(name = "logMongoTemplate")
    public MongoTemplate mongoTemplate() throws Exception {
        String uri = productList.contains("dfs") ? logUri : defaultUri;
        MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(logUri));

        // check monitoringLogs index
        MongoCollection<Document> monitoringLogs = mongoTemplate.getCollection("monitoringLogs");
        if (monitoringLogs.countDocuments() == 0) {
            monitoringLogs.dropIndexes();

            monitoringLogs.createIndex(Indexes.compoundIndex(new BsonDocument("taskId", new BsonInt32(1)),
                    new BsonDocument("taskRecordId", new BsonInt32(1)),
                    new BsonDocument("nodeId", new BsonInt32(1)),
                    new BsonDocument("level", new BsonInt32(1))));

            monitoringLogs.createIndex(new BsonDocument("date", new BsonInt32(-1)), new IndexOptions().expireAfter(7L, TimeUnit.DAYS));
        }

        return mongoTemplate;
    }
}
