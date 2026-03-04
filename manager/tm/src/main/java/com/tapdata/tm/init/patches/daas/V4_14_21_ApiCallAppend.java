package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
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

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/22 18:38 Create
 * @description
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.14-21")
public class V4_14_21_ApiCallAppend extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_14_21_ApiCallAppend.class);
    public V4_14_21_ApiCallAppend(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        if (mongoTemplate == null) {
            logger.error("MongoTemplate bean not found, patch execution failed");
            return;
        }
        try {
            String apiCallName = MongoUtils.getCollectionName(ApiCallEntity.class);
            MongoCollection<Document> collection = mongoTemplate.getCollection(apiCallName);
            Document query = new Document();
            query.append("delete", new Document().append("$exists", false));
            collection.updateMany(query, new Document().append("$set", new Document("delete", false)));
        } catch (Exception e) {
            logger.error("Failed to append delete field to api call collection", e);
        }
    }
}
