package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/4/28 19:23 Create
 * @description
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.17-6")
public class V4_17_6_RemoveUselessSystemConfig extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_17_6_RemoveUselessSystemConfig.class);

    public V4_17_6_RemoveUselessSystemConfig(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        assert null != mongoTemplate;
        MongoCollection<Document> collection = mongoTemplate.getCollection("Settings");
        try {
            collection.deleteMany(new Document().append("key", new Document().append("$in", List.of("dataSourcePoolConfig", "accessTimeout"))));
            collection.deleteMany(new Document().append("$or", List.of(new Document().append("category", new Document().append("$exists", false)), new Document().append("category", null))));
        } catch (Exception e) {
            logger.warn("Unable remove useless system config, key: dataSourcePoolConfig, accessTimeout, {}", e.getMessage());
        }
    }
}
