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

@PatchAnnotation(appType = AppType.DAAS, version = "3.13-1")
public class V3_13_1_TaskPipelineLimit_PipelineId_Index extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V3_13_1_TaskPipelineLimit_PipelineId_Index.class);

    public V3_13_1_TaskPipelineLimit_PipelineId_Index(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "TaskPipelineLimit";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        Index index = new Index().named("pipelineId_unique").on("pipelineId", Sort.Direction.DESC).unique().background();
        mongoTemplate.indexOps(collectionName).ensureIndex(index);
    }
}
