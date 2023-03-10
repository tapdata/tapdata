package com.tapdata.tm.init.patches.daas;

import com.mongodb.BasicDBObject;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.tapdata.tm.ds.entity.DataSourceDefinitionEntity;
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
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.OutOperation;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;

import java.util.List;
import java.util.Objects;

/**
 *
 * @author <a href="mailto:meteor209@gmail.com">jiuye</a>
 * @version v1.0 2023/2/10 15:47 Create
 */
@PatchAnnotation(appType = AppType.DAAS, version = "2.14-5")
public class V2_14_5_DatabaseTypes extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V2_14_5_DatabaseTypes.class);

    public V2_14_5_DatabaseTypes(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "DatabaseTypes";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        GridFsTemplate gridFsTemplate = SpringContextHelper.getBean(GridFsTemplate.class);

        //bak
        OutOperation outOperation = new OutOperation("DatabaseTypes_bak_2_14");
        mongoTemplate.aggregate(Aggregation.newAggregation(outOperation), collectionName, BasicDBObject.class);

        mongoTemplate.indexOps(collectionName).getIndexInfo().forEach(index -> {
            if ("index_pdkHash_1_pdkAPIBuildNumber_1".equals(index.getName())) {
                mongoTemplate.indexOps(collectionName).dropIndex("index_pdkHash_1_pdkAPIBuildNumber_1");
            } else if ("pdkHash_1_pdkAPIBuildNumber_1".equals(index.getName())) {
                mongoTemplate.indexOps(collectionName).dropIndex("pdkHash_1_pdkAPIBuildNumber_1");
            }
        });

        // del data
        mongoTemplate.remove(Query.query(Criteria.where("pdkId").exists(false)), collectionName);
        List<DataSourceDefinitionEntity> all = mongoTemplate.findAll(DataSourceDefinitionEntity.class);
        all.forEach(info -> {
            if (info.isLatest()) {
                GridFSFile one = gridFsTemplate.findOne(Query.query(Criteria.where("_id").is(info.getJarRid())));
                if (Objects.isNull(one)) {
                    mongoTemplate.remove(Query.query(Criteria.where("_id").is(info.getId())), DataSourceDefinitionEntity.class);
                }
            } else {
                mongoTemplate.remove(Query.query(Criteria.where("_id").is(info.getId())), DataSourceDefinitionEntity.class);
            }
        });

        // create union index
        CompoundIndexDefinition compoundIndex = new CompoundIndexDefinition(new Document()
                .append("pdkHash", 1).append("pdkAPIBuildNumber",1));
        compoundIndex.unique();
        mongoTemplate.indexOps(collectionName).ensureIndex(compoundIndex);
    }
}
