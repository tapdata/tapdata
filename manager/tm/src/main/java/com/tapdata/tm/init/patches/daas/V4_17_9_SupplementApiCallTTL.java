package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
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

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/27 19:57 Create
 * @description
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.17-9")
public class V4_17_9_SupplementApiCallTTL extends V4_14_21_AddApiCallTTL {
    private static final Logger logger = LogManager.getLogger(V4_17_9_SupplementApiCallTTL.class);

    public V4_17_9_SupplementApiCallTTL(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        assert mongoTemplate != null;
        String apiCallName = MongoUtils.getCollectionName(ApiCallEntity.class);
        dropIndexIfNeed(mongoTemplate, apiCallName, "createTime_1");
        dropIndexIfNeed(mongoTemplate, apiCallName, "received_date_1");
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_1_ttl", new Document(CREATE_TIME, 1), 2592000L);
    }

    protected void dropIndexIfNeed(MongoTemplate mongoTemplate, String collectionName, String indexName) {
        List<Document> indexList = mongoTemplate.getCollection(collectionName)
                .listIndexes()
                .into(new ArrayList<>());
        final boolean indexExists = indexList.stream()
                .anyMatch(doc -> indexName.equals(doc.getString("name")));
        if (!indexExists) {
            return;
        }
        try {
            mongoTemplate.getCollection(collectionName).dropIndex(indexName);
        } catch (Exception e) {
            logger.error("Failed to drop index {}", indexName, e);
        }
    }
}
