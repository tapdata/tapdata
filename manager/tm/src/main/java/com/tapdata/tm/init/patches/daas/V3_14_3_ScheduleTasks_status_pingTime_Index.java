package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

@PatchAnnotation(appType = AppType.DAAS, version = "3.14-3")
public class V3_14_3_ScheduleTasks_status_pingTime_Index extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V3_14_3_ScheduleTasks_status_pingTime_Index.class);

    public V3_14_3_ScheduleTasks_status_pingTime_Index(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "ScheduleTasks";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        Index index = new Index().named("status_1_ping_time_1").on("status", Sort.Direction.ASC).on("ping_time", Sort.Direction.ASC).background();
        mongoTemplate.indexOps(collectionName).ensureIndex(index);
    }

}
