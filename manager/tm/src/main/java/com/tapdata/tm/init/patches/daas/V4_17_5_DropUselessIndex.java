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
 * @version v1.0 2026/4/14 17:35 Create
 * @description
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.17-5")
public class V4_17_5_DropUselessIndex extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_17_5_DropUselessIndex.class);
    public V4_17_5_DropUselessIndex(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        assert null != mongoTemplate;
        String apiCallName = MongoUtils.getCollectionName(ApiCallEntity.class);
        MongoCollection<Document> collection = mongoTemplate.getCollection(apiCallName);
        try {
            collection.dropIndex("createTime_1_hasMetric_1_delete_1");
        } catch (Exception e) {
            logger.warn("drop index not succeed, may createTime_1_hasMetric_1_delete_1 not exists");
        }
    }
}
