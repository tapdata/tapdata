package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/12 12:09 Create
 * @description
 * [
 *   {
 *     "createIndexes": "ApiCall",
 *     "indexes": [
 *       {
 *         "key": {
 *           "allPathId": -1,
 *           "createTime": -1,
 *           "_id": 1
 *         },
 *         "name": "ApiCall_query-1",
 *         "background": true
 *       }
 *     ]
 *   },
 *   {
 *     "createIndexes": "ApiCall",
 *     "indexes": [
 *       {
 *         "key": {
 *           "workOid": 1,
 *           "reqTime": 1
 *         },
 *         "name": "ApiCall_query-2",
 *         "background": true
 *       }
 *     ]
 *   },
 *   {
 *     "createIndexes": "ApiCall",
 *     "indexes": [
 *       {
 *         "key": {
 *           "supplement": 1,
 *           "createTime": 1,
 *           "allPathId": 1,
 *           "reqTime": 1
 *         },
 *         "name": "ApiCall_query_supplement-3",
 *         "background": true
 *       }
 *     ]
 *   },
 *   {
 *     "createIndexes": "ApiCallInWorker",
 *     "indexes": [
 *       {
 *         "key": {
 *           "workOid": 1,
 *           "timeGranularity": 1,
 *           "timeStart": -1
 *         },
 *         "name": "ApiCallInWorker_query-1",
 *         "background": true
 *       }
 *     ]
 *   },
 *   {
 *     "createIndexes": "ApiCall",
 *     "indexes": [{
 *       "key": {
 *         "createTime": 1
 *       },
 *       "expireAfterSeconds": 2592000,
 *       "name": "ApiCall_1_ttl",
 *       "background": true
 *     }]
 *   }
 * ]
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.9-10")
public class V4_9_10_AddApiCallTTL extends AbsPatch {
    public static final String CREATE_TIME = "createTime";
    private static final Logger logger = LogManager.getLogger(V4_9_10_AddApiCallTTL.class);

    public V4_9_10_AddApiCallTTL(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        if (mongoTemplate == null) {
            logger.error("MongoTemplate bean not found, patch execution failed");
            return;
        }
        String apiCallName = MongoUtils.getCollectionName(ApiCallEntity.class);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_1_ttl", new Document(CREATE_TIME, 1), 2592000L);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_query-1", new Document("allPathId", -1).append(CREATE_TIME, -1).append("_id", 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_query-2", new Document("workOid", 1).append("reqTime", 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_query_supplement-3", new Document("supplement", 1).append(CREATE_TIME, 1).append("allPathId", 1).append("reqTime", 1), null);

        String workerCallName = MongoUtils.getCollectionName(WorkerCallEntity.class);
        createTTLIndexIfNeed(mongoTemplate, workerCallName, "ApiCallInWorker_query-1", new Document("workOid", 1).append("timeGranularity", 1).append("timeStart", -1), null);
    }

    protected void createTTLIndexIfNeed(MongoTemplate mongoTemplate, String collectionName, String indexName, Document indexKeyMap, Long expireAfterSeconds) {
        List<Document> indexList = mongoTemplate.getCollection(collectionName)
                .listIndexes()
                .into(new ArrayList<>());
        final boolean indexExists = indexList.stream()
                .anyMatch(doc -> indexName.equals(doc.getString("name")));
        if (indexExists) {
            logger.info("Index {} already exists in collection {}, skipping creation", indexName, collectionName);
            return;
        }
        IndexOptions options = new IndexOptions()
                .name(indexName)
                .background(true);
        Optional.ofNullable(expireAfterSeconds).ifPresent(ts -> options.expireAfter(ts, TimeUnit.SECONDS));
        try {
            mongoTemplate.getCollection(collectionName).createIndex(indexKeyMap, options);
        } catch (Exception e) {
            logger.error("Failed to create index {}", indexName, e);
        }
    }
}
