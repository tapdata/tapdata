package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.entity.field.ModulesField;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import com.tapdata.tm.worker.entity.field.ServerUsageField;
import com.tapdata.tm.worker.entity.field.ServerUsageMetricField;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

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
        loadingClientId(collection);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_reqTime_-1", new Document(ApiCallField.REQ_TIME.field(), -1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_latency_-1", new Document(ApiCallField.LATENCY.field(), -1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_dataQueryTotalTime_-1", new Document(ApiCallField.DATA_QUERY_TOTAL_TIME.field(), -1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_allPathId_req_path_method_1_reqTime_-1", new Document(ApiCallField.CLIENT_ID.field(), 1).append(ApiCallField.ALL_PATH_ID.field(), 1).append(ApiCallField.REQ_PATH.field(), 1).append(ApiCallField.METHOD.field(), 1).append(ApiCallField.SUCCEED.field(), 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_delete_1_api_gateway_uuid_1_reqTime_1", new Document(ApiCallField.API_GATEWAY_UUID.field(), 1).append(ApiCallField.REQ_TIME.field(), 1).append(ApiCallField.DELETE.field(), 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_apiId_reqTime_dbCost_latency_code_status_delete", new Document(ApiCallField.ALL_PATH_ID.field(), 1).append(ApiCallField.REQ_TIME.field(), 1).append(ApiCallField.DATA_QUERY_TOTAL_TIME.field(), 1).append(ApiCallField.LATENCY.field(), 1).append(ApiCallField.SUCCEED.field(), 1).append(ApiCallField.DELETE.field(), 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_apiId_-1_createTime_1_callId_1", new Document(ApiCallField.ALL_PATH_ID.field(), -1).append(BaseEntityFields.CREATE_TIME.field(), -1).append(BaseEntityFields._ID.field(), 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_workerId_1_reqTime_1", new Document(ApiCallField.WORK_O_ID.field(), 1).append(ApiCallField.REQ_TIME.field(), 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_supplement-1_createTime_1_apiId_1_reqTime_1", new Document(ApiCallField.SUPPLEMENT.field(), 1).append(BaseEntityFields.CREATE_TIME.field(), 1).append(ApiCallField.ALL_PATH_ID.field(), 1).append(ApiCallField.REQ_TIME.field(), 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_hasMetric_1_createTime_1_reqTime_1", new Document(ApiCallField.HAS_METRIC.field(), 1).append(BaseEntityFields.CREATE_TIME.field(), 1).append(ApiCallField.REQ_TIME.field(), 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_serverId_1_supplement_1_apiId_1_workerId_1_callId_1", new Document(ApiCallField.API_GATEWAY_UUID.field(), 1).append(BaseEntityFields._ID.field(), 1).append(ApiCallField.SUPPLEMENT.field(), 1).append(ApiCallField.REQ_PATH.field(), 1).append(ApiCallField.ALL_PATH_ID.field(), 1).append(ApiCallField.WORK_O_ID.field(), 1).append(ApiCallField.SUCCEED.field(), 1), null);
        createTTLIndexIfNeed(mongoTemplate, apiCallName, "ApiCall_1_ttl", new Document(BaseEntityFields.CREATE_TIME.field(), 1), 2592000L);
        String serverUsageMetricName = MongoUtils.getCollectionName(ServerUsageMetric.class);
        dropInvalidRecords(mongoTemplate, serverUsageMetricName);
        createTTLIndexIfNeed(mongoTemplate, serverUsageMetricName, "ServerUsageMetric_granularity_1_lastUpdateTime_-1", new Document(ServerUsageMetricField.TIME_GRANULARITY.field(), 1).append(ServerUsageField.LAST_UPDATE_TIME.field(), 1), null);
        String serverUsageName = MongoUtils.getCollectionName(ServerUsage.class);
        createTTLIndexIfNeed(mongoTemplate, serverUsageName, "ServerUsage_lastUpdateTime_1", new Document(ServerUsageField.LAST_UPDATE_TIME.field(), 1), null);
        String modulesName = MongoUtils.getCollectionName(ModulesEntity.class);
        createTTLIndexIfNeed(mongoTemplate, modulesName, "Modules_status_1_is_deleted_1", new Document(ModulesField.STATUS.field(), 1).append(ModulesField.IS_DELETED.field(), 1), null);
    }

    protected void loadingClientId(MongoCollection<Document> collection) {
        Document query = new Document().append("user_info.clientId", new Document().append("$exists", true));
        Document update = new Document().append("$set", List.of(new Document().append("clientId", "$user_info.clientId")));
        try {
            collection.updateMany(query, update);
        } catch (Exception e) {
            logger.warn("update api call set clientId by user_info.clientId failed: {}", e.getMessage());
        }
    }

    protected void dropInvalidRecords(MongoTemplate mongoTemplate, String collectionName) {
        long ts = System.currentTimeMillis();
        try {
            mongoTemplate.remove(new Document().append("lastUpdateTime", new Document("$gt", ts)), collectionName);
        } catch (Exception e) {
            logger.warn("Unable delete dropInvalid records, please delete by command: db.{}.deleteMany({\"lastUpdateTime\": {\"gt\": {}}})", collectionName, ts);
        }
    }
}
