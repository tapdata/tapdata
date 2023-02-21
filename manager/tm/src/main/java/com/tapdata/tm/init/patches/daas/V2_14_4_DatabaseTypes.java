package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

/**
 *
 * @author <a href="mailto:meteor209@gmail.com">jiuye</a>
 * @version v1.0 2023/2/10 15:47 Create
 */
@PatchAnnotation(appTypes = {AppType.DAAS}, version = "2.14-4")
public class V2_14_4_DatabaseTypes extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V2_14_4_DatabaseTypes.class);

    public V2_14_4_DatabaseTypes(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "DatabaseTypes";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        boolean match = mongoTemplate.indexOps(collectionName).getIndexInfo().stream()
                .anyMatch(index -> "index_pdkHash_1_pdkAPIBuildNumber_1".equals(index.getName()));
        if (match) {
            mongoTemplate.indexOps(collectionName).dropIndex("index_pdkHash_1_pdkAPIBuildNumber_1");
        }
        // del data
        mongoTemplate.remove(Query.query(Criteria.where("pdkId").exists(false)), collectionName);
        mongoTemplate.remove(Query.query(Criteria.where("latest").is(false)), collectionName);

        // create union index
        CompoundIndexDefinition compoundIndex = new CompoundIndexDefinition(new Document()
                .append("pdkHash", 1).append("pdkAPIBuildNumber",1));
        compoundIndex.unique();
        mongoTemplate.indexOps(collectionName).ensureIndex(compoundIndex);
    }
}
