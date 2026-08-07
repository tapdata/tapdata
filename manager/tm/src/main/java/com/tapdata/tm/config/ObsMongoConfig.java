package com.tapdata.tm.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import io.tapdata.mongodb.utils.SSLUtil;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.data.mongodb.SpringDataMongoDB;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableAsync
@EnableMongoRepositories(mongoTemplateRef = "obsMongoTemplate", basePackages = {"com.tapdata.tm.monitor.service"})
public class ObsMongoConfig implements AsyncConfigurer {
    private static final Logger logger = LoggerFactory.getLogger(ObsMongoConfig.class);

    static final String IDX_GRNTY_TYPE_TASK_ID_DATE = "idx_grnty_type_taskId_date";
    static final String IDX_TASK_ID_GRNTY_TYPE_DATE = "idx_taskId_grnty_type_date";

    @Value("${spring.data.mongodb.obs.uri}")
    private String obsUri;
    @Value("${spring.data.mongodb.default.uri}")
    private String defaultUri;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private List<String> productList;
    @Value("${spring.data.mongodb.ssl}")
    private boolean ssl;
    @Value("${spring.data.mongodb.caPath}")
    private String caPath;
    @Value("${spring.data.mongodb.keyPath}")
    private String keyPath;
    @Value("${spring.data.mongodb.sslPass}")
    private String sslPass;
    @Bean(name = "obsMongoTemplate")
    public CompletableFuture<MongoTemplate> mongoTemplate() throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String uri = productList.contains("dfs") ? obsUri : defaultUri;
                MongoTemplate mongoTemplate;
                if (ssl) {
                    String database = new ConnectionString(uri).getDatabase();
                    MongoClientSettings settings = SSLUtil.mongoClientSettings(ssl, keyPath, caPath, sslPass, uri);
                    MongoClient mongoClient = MongoClients.create(settings, SpringDataMongoDB.driverInformation());
                    mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(mongoClient, database));
                } else {
                    mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(uri));
                }
                MongoCollection<Document> agentMeasurementV2 = mongoTemplate.getCollection(MeasurementEntity.COLLECTION_NAME);
                initializeBootstrapIndexes(agentMeasurementV2);
                initializeDashboardIndexesAsync(agentMeasurementV2);

                return mongoTemplate;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void initializeAgentMeasurementIndexes(MongoCollection<Document> agentMeasurementV2) {
        initializeBootstrapIndexes(agentMeasurementV2);
        initializeDashboardIndexes(agentMeasurementV2);
    }

    private void initializeBootstrapIndexes(MongoCollection<Document> agentMeasurementV2) {
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

            agentMeasurementV2.createIndex(new BsonDocument("date", new BsonInt32(-1)),
                    new IndexOptions().expireAfter(7L, TimeUnit.DAYS));
        }
    }

    CompletableFuture<Void> initializeDashboardIndexesAsync(MongoCollection<Document> agentMeasurementV2) {
        try {
            return CompletableFuture.runAsync(() -> initializeDashboardIndexes(agentMeasurementV2));
        } catch (RuntimeException e) {
            logger.error("Unable to schedule AgentMeasurementV2 dashboard index initialization", e);
            return CompletableFuture.completedFuture(null);
        }
    }

    void initializeDashboardIndexes(MongoCollection<Document> agentMeasurementV2) {
        List<Document> existingIndexes;
        try {
            existingIndexes = agentMeasurementV2.listIndexes().into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Unable to inspect AgentMeasurementV2 indexes; dashboard index initialization skipped", e);
            return;
        }

        ensureIndexSafely(agentMeasurementV2, existingIndexes, IDX_GRNTY_TYPE_TASK_ID_DATE,
                new Document("grnty", 1)
                        .append("tags.type", 1)
                        .append("tags.taskId", 1)
                        .append("date", 1));
        ensureIndexSafely(agentMeasurementV2, existingIndexes, IDX_TASK_ID_GRNTY_TYPE_DATE,
                new Document("tags.taskId", 1)
                        .append("grnty", 1)
                        .append("tags.type", 1)
                        .append("date", -1));
    }

    private void ensureIndexSafely(MongoCollection<Document> collection, List<Document> existingIndexes,
                                   String indexName, Document indexKeys) {
        try {
            ensureIndex(collection, existingIndexes, indexName, indexKeys);
        } catch (Exception e) {
            logger.error("Unable to initialize AgentMeasurementV2 index {}", indexName, e);
        }
    }

    private void ensureIndex(MongoCollection<Document> collection, List<Document> existingIndexes,
                             String indexName, Document indexKeys) {
        for (Document existingIndex : existingIndexes) {
            Document existingKeys = existingIndex.get("key", Document.class);
            if (indexName.equals(existingIndex.getString("name"))) {
                if (!sameIndexKeys(existingKeys, indexKeys)) {
                    logger.warn("Index {} already exists in collection {} with different keys, skipping creation. "
                                    + "Existing keys: {}, expected keys: {}",
                            indexName, MeasurementEntity.COLLECTION_NAME, existingKeys, indexKeys);
                } else {
                    logger.info("Index {} already exists in collection {}, skipping creation",
                            indexName, MeasurementEntity.COLLECTION_NAME);
                }
                return;
            }
            if (sameIndexKeys(existingKeys, indexKeys)) {
                logger.warn("Index {} already exists in collection {} as {}, skipping creation",
                        indexName, MeasurementEntity.COLLECTION_NAME, existingIndex.getString("name"));
                return;
            }
        }

        collection.createIndex(indexKeys, new IndexOptions().name(indexName));
        existingIndexes.add(new Document("name", indexName).append("key", indexKeys));
        logger.info("Created index {} in collection {}", indexName, MeasurementEntity.COLLECTION_NAME);
    }

    private boolean sameIndexKeys(Document left, Document right) {
        if (left == null || right == null || left.size() != right.size()) {
            return false;
        }
        List<Map.Entry<String, Object>> leftEntries = new ArrayList<>(left.entrySet());
        List<Map.Entry<String, Object>> rightEntries = new ArrayList<>(right.entrySet());
        for (int i = 0; i < leftEntries.size(); i++) {
            Map.Entry<String, Object> leftEntry = leftEntries.get(i);
            Map.Entry<String, Object> rightEntry = rightEntries.get(i);
            if (!Objects.equals(leftEntry.getKey(), rightEntry.getKey())
                    || !sameIndexValue(leftEntry.getValue(), rightEntry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private boolean sameIndexValue(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue()) == 0;
        }
        return Objects.equals(left, right);
    }
}
