package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
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

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/4/14 17:35 Create
 * @description
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.17-6")
public class V4_17_6_ResetApiCallIndex extends V4_14_21_AddApiCallTTL {
    private static final Logger logger = LogManager.getLogger(V4_17_6_ResetApiCallIndex.class);

    public V4_17_6_ResetApiCallIndex(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        assert null != mongoTemplate;
        String apiCallName = MongoUtils.getCollectionName(ApiCallEntity.class);
        MongoCollection<Document> collection = mongoTemplate.getCollection(apiCallName);
        try {
            collection.dropIndexes();
        } catch (Exception e) {
            logger.warn("drop index not succeed: {}", e.getMessage());
        }
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_1_ttl", new Document(CREATE_TIME, 1), 2592000L);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "apiId_reqTime_dbCost_latency_code_status_delete", new Document("allPathId", 1).append("reqTime", 1).append("dataQueryTotalTime", 1).append("latency", 1).append("succeed", 1).append("delete", 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_query-1", new Document("allPathId", -1).append("createTime", -1).append("_id", 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_query-2", new Document("workOid", 1).append("reqTime", 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_query_supplement-3", new Document("supplement", 1).append("createTime", 1).append("allPathId", 1).append("reqTime", 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_metric_filter_sort", new Document("hasMetric", 1).append("createTime", 1).append("reqTime", 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "delete_1_api_gateway_uuid_1_reqTime_1", new Document("api_gateway_uuid", 1).append("reqTime", 1).append("delete", 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_idx_covering_query", new Document("api_gateway_uuid", 1).append("supplement", 1).append("allPathId", 1).append("workOid", 1).append("_id", 1).append("code", 1).append("codeMsg", 1).append("httpStatus", 1).append("succeed", 1), null);
    }
}
