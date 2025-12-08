package com.tapdata.tm.config;

import com.mongodb.MongoException;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Configuration
public class CustomMongoConfig {

    private static final Logger logger = LoggerFactory.getLogger(CustomMongoConfig.class);

    private final MongoTemplate mongoTemplate;

    public CustomMongoConfig(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    protected int batchSize = 2000;

    protected int batch() {
        return batchSize;
    }

    @PostConstruct
    public void init() {
        ClassPathScanningCandidateComponentProvider provider =
                new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AnnotationTypeFilter(CappedCollection.class));
        provider.findCandidateComponents("com.tapdata.tm")
                .forEach(this::eachOnce);
    }

    protected void eachOnce(BeanDefinition beanDef) {
        try {
            Class<?> clazz = Class.forName(beanDef.getBeanClassName());
            Document entityAnnotation = clazz.getAnnotation(Document.class);
            if (null == entityAnnotation) {
                return;
            }
            CappedCollection annotation = clazz.getAnnotation(CappedCollection.class);
            if (null == annotation) {
                return;
            }
            String name = entityAnnotation.value();
            if (annotation.capped()) {
                handleCappedCollection(name, annotation);
            } else {
                handleNonCappedCollection(name);
            }
        } catch (Exception e) {
            logger.warn("Failed to process capped collection for class: {}, error: {}",
                    beanDef.getBeanClassName(), e.getMessage());
        }
    }

    protected void handleNonCappedCollection(String collectionName) {
        try {
            if (mongoTemplate.collectionExists(collectionName)) {
                CollectionStats stats = getCollectionStats(collectionName);
                if (stats.isCapped()) {
                    changeToNonCappedCollection(collectionName);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to handle capped collection: {}, error: {}", collectionName, e.getMessage(), e);
        }
    }

    protected void changeToNonCappedCollection(String collectionName) {
        try {
            backupAndRecreateCollection(collectionName, () -> {
                logger.info("Creating new non-capped collection: {}", collectionName);
                mongoTemplate.createCollection(collectionName);
                logger.info("Successfully created non-capped collection: {}", collectionName);
            });
        } catch (Exception e) {
            logger.error("Failed to change capped collection to non-capped: {}, error: {}", collectionName, e.getMessage(), e);
        }
    }

    protected void handleCappedCollection(String collectionName, CappedCollection annotation) {
        try {
            if (!mongoTemplate.collectionExists(collectionName)) {
                createCappedCollection(collectionName, annotation);
            } else {
                updateExistingCollection(collectionName, annotation);
            }
        } catch (Exception e) {
            logger.error("Failed to handle capped collection: {}, error: {}", collectionName, e.getMessage(), e);
        }
    }

    protected void createCappedCollection(String collectionName, CappedCollection annotation) {
        logger.info("Creating new capped collection: {}", collectionName);
        CollectionOptions collectionOptions = CollectionOptions.empty()
                .capped();
        long length = annotation.maxLength();
        if (length <= 0L) {
            length = 100000L;
        }
        long size = annotation.maxMemory();
        if (size <= 0L) {
            size = 1L << 40;
        }
        CollectionOptions options = collectionOptions.size(size).maxDocuments(length);
        mongoTemplate.createCollection(collectionName, options);
        logger.info("Successfully created capped collection: {}", collectionName);
    }

    protected void updateExistingCollection(String collectionName, CappedCollection annotation) {
        try {
            CollectionStats stats = getCollectionStats(collectionName);
            if (!stats.isCapped()) {
                backupAndRecreateCollection(collectionName, () -> createCappedCollection(collectionName, annotation));
            } else {
                if (needsUpdate(stats, annotation)) {
                    backupAndRecreateCollection(collectionName, () -> createCappedCollection(collectionName, annotation));
                } else {
                    logger.debug("Capped collection {} attributes are up to date", collectionName);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to update existing collection: {}, error: {}", collectionName, e.getMessage(), e);
        }
    }

    protected CollectionStats getCollectionStats(String collectionName) {
        try {
            MongoDatabase database = mongoTemplate.getDb();
            org.bson.Document collStats = database.runCommand(
                    new org.bson.Document("collStats", collectionName)
            );

            boolean capped = collStats.getBoolean("capped", false);
            long maxSize = Optional.ofNullable(collStats.get("maxSize"))
                    .map(String::valueOf)
                    .map(Long::parseLong)
                    .orElse(-1L);
            long max = Optional.ofNullable(collStats.get("max"))
                    .map(String::valueOf)
                    .map(Long::parseLong)
                    .orElse(0L);

            return new CollectionStats(capped, maxSize, max);
        } catch (Exception e) {
            logger.error("Failed to get collection stats for: {}, error: {}", collectionName, e.getMessage());
            return new CollectionStats(false, 0L, 0L);
        }
    }

    protected boolean needsUpdate(CollectionStats stats, CappedCollection annotation) {
        if (annotation.maxMemory() > 0L) {
            return stats.getMax() != annotation.maxLength() ||
                    stats.getMaxSize() != annotation.maxMemory();
        }
        return stats.getMax() != annotation.maxLength();
    }

    protected void backupAndRecreateCollection(String collectionName, Runnable createCollection) {
        try {
            String backupCollectionName = collectionName + "_backup_" + System.currentTimeMillis();
            MongoDatabase database = mongoTemplate.getDb();
            MongoCollection<org.bson.Document> collection = database.getCollection(collectionName);
            collection.renameCollection(
                    new com.mongodb.MongoNamespace(database.getName(), backupCollectionName)
            );
            createCollection.run();
            MongoCollection<org.bson.Document> newCollection = database.getCollection(collectionName);
            MongoCollection<org.bson.Document> backupCollection = database.getCollection(backupCollectionName);
            long beforeCount = backupCollection.countDocuments();
            List<org.bson.Document> buffer = new ArrayList<>(batch());
            try (MongoCursor<org.bson.Document> cursor = backupCollection.find().iterator()) {
                while (cursor.hasNext()) {
                    buffer.add(cursor.next());
                    if (buffer.size() == batch()) {
                        if (!insertMany(newCollection, buffer)) {
                            break;
                        }
                        buffer = new ArrayList<>();
                    }
                }
            } finally {
                if (!buffer.isEmpty()) {
                    insertMany(newCollection, buffer);
                }
            }
            long afterCount = newCollection.countDocuments();
            ListIndexesIterable<org.bson.Document> indexesIterable = backupCollection.listIndexes();
            List<org.bson.Document> indexes = new ArrayList<>();
            for (org.bson.Document document : indexesIterable) {
                if (document.getString("name").equals("_id_")) {
                    continue;
                }
                indexes.add(document);
            }
            if (afterCount >= beforeCount || backupCollection.countDocuments() <= 0L) {
                backupCollection.drop();
            }
            syncIndex(indexes, newCollection);
        } catch (Exception e) {
            logger.error("Failed to recreate capped collection {}: {}", collectionName, e.getMessage(), e);
        }
    }

    protected void syncIndex(List<org.bson.Document> indexes, MongoCollection<org.bson.Document> newCollection) {
        for (org.bson.Document listIndex : indexes) {
            try {
                newCollection.createIndex(listIndex.get("key", org.bson.Document.class));
            } catch (Exception e) {
                logger.error("Failed to create index: {}, error: {}", listIndex, e.getMessage());
            }
        }
    }

    boolean insertMany(MongoCollection<org.bson.Document> newCollection, List<org.bson.Document> buffer) {
        try {
            newCollection.insertMany(buffer, new InsertManyOptions().ordered(false));
        } catch (MongoException e) {
            logger.warn("Batch insert reached capped collection limits, stopping migration.");
            return false;
        }
        return true;
    }


    @Getter
    public static class CollectionStats {
        private final boolean capped;
        private final long maxSize;
        private final long max;

        public CollectionStats(boolean capped, long maxSize, long max) {
            this.capped = capped;
            this.maxSize = maxSize;
            this.max = max;
        }
    }
}
