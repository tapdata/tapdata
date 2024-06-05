package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import java.util.concurrent.TimeUnit;
@PatchAnnotation(appType = AppType.DAAS, version = "3.8-1")
public class v3_8_1_AccessToken_TTL_Index extends AbsPatch {
    private Long accessTokenTtl;
    private static final Logger logger = LogManager.getLogger(v3_8_1_AccessToken_TTL_Index.class);
    public v3_8_1_AccessToken_TTL_Index(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "AccessToken";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        Environment environment = SpringContextHelper.getBean(Environment.class);
        Integer ttl = Integer.valueOf(environment.getProperty("access.token.ttl"));
        accessTokenTtl = ttl.longValue();

        long buffer = 86400L;
        long ttlValue = accessTokenTtl + buffer;

        Index index = new Index().on("last_updated", Sort.Direction.DESC).expire(ttlValue, TimeUnit.SECONDS).background();
        mongoTemplate.indexOps(collectionName).ensureIndex(index);
    }

}
