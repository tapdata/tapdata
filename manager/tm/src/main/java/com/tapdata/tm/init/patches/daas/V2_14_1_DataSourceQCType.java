package com.tapdata.tm.init.patches.daas;

import io.tapdata.pdk.core.constants.DataSourceQCType;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.HashMap;
import java.util.Map;

/**
 * Changed data source quality control type maintenance, from beta to qcType.
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/10 15:47 Create
 */
@PatchAnnotation(appType = AppType.DAAS, version = "2.14-1")
public class V2_14_1_DataSourceQCType extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V2_14_1_DataSourceQCType.class);

    public V2_14_1_DataSourceQCType(PatchType patchType, PatchVersion version) {
        super(patchType, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "DatabaseTypes";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        Map<DataSourceQCType, Long> results = new HashMap<>();
        // beta and not-set the qcType is Alpha
        results.put(DataSourceQCType.Alpha, mongoTemplate.updateMulti(
                Query.query(Criteria.where("qcType").exists(false).orOperator(
                        Criteria.where("beta").is(true)
                        , Criteria.where("beta").exists(false)
                ))
                , Update.update("qcType", DataSourceQCType.Alpha).unset("beta")
                , collectionName
        ).getModifiedCount());
        // beta=false the qcType is GA
        results.put(DataSourceQCType.GA, mongoTemplate.updateMulti(
                Query.query(Criteria.where("qcType").exists(false).and("beta").exists(true))
                , Update.update("qcType", DataSourceQCType.GA).unset("beta")
                , collectionName
        ).getModifiedCount());

        for (DataSourceQCType t : DataSourceQCType.values()) {
            if (results.getOrDefault(t, 0L) > 0) {
                logger.info("Complete the qcType repair, update {}", results);
                break;
            }
        }
    }
}
