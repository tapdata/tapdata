package com.tapdata.tm.config;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Collections;

/**
 * Statistics for callers that do not consume sharding metadata.
 * Schema discovery and sharding detection must keep their existing command path.
 */
public final class MongoCollectionStatistics {
    private Boolean useAggregation;
    private Object versionScope;

    public Document read(MongoDatabase database, String collection) {
        return read(database, collection, database);
    }

    public Document read(MongoDatabase database, String collection, Object connectionScope) {
        if (!supportsAggregation(database, connectionScope)) {
            return database.runCommand(new Document("collStats", collection));
        }
        Document result = null;
        long count = 0;
        long size = 0;
        long storageSize = 0;
        int rows = 0;
        try (MongoCursor<Document> cursor = database.getCollection(collection)
                .aggregate(Collections.singletonList(
                        new Document("$collStats", new Document("storageStats", new Document()))))
                .iterator()) {
            while (cursor.hasNext()) {
                Document stats = cursor.next().get("storageStats", Document.class);
                if (stats == null) {
                    throw new IllegalStateException("MongoDB collection statistics are missing");
                }
                if (result == null) {
                    result = new Document(stats);
                }
                count += number(stats, "count");
                size += number(stats, "size");
                storageSize += number(stats, "storageSize");
                rows++;
            }
        } catch (MongoCommandException e) {
            // Legacy collStats can return zero statistics for a missing collection.
            // Preserve that server-specific contract instead of inventing a result.
            if (e.getErrorCode() == 26) {
                return database.runCommand(new Document("collStats", collection));
            }
            throw e;
        }
        if (result == null) {
            throw new IllegalStateException("MongoDB collection statistics are empty");
        }
        if (rows > 1) {
            result.put("count", count);
            result.put("size", size);
            result.put("storageSize", storageSize);
            result.put("avgObjSize", count == 0 ? 0D : (double) size / count);
        }
        return result;
    }

    private synchronized boolean supportsAggregation(MongoDatabase database, Object connectionScope) {
        Boolean supported = useAggregation;
        if (supported != null && versionScope == connectionScope) {
            return supported;
        }
        try {
            String version = database.runCommand(new Document("buildInfo", 1)).getString("version");
            supported = Integer.parseInt(version.split("\\.")[0]) >= 6;
            useAggregation = supported;
            versionScope = connectionScope;
            return supported;
        } catch (RuntimeException ignored) {
            // Version discovery must not become a new permission prerequisite.
            return false;
        }
    }

    private static long number(Document stats, String field) {
        Number value = (Number) stats.get(field);
        return value == null ? 0L : value.longValue();
    }
}

