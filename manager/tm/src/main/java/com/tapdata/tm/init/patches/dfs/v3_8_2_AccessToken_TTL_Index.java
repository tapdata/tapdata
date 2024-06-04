package com.tapdata.tm.init.patches.dfs;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@PatchAnnotation(appType = AppType.DFS, version = "3.8-2")
public class v3_8_2_AccessToken_TTL_Index extends AbsPatch {
    private static Long accessTokenTtl;
    @Value("${access.token.ttl}")
    private void setAccessTokenTtl(long value){
        accessTokenTtl = value;
    }
    private static final Logger logger = LogManager.getLogger(v3_8_2_AccessToken_TTL_Index.class);
    public v3_8_2_AccessToken_TTL_Index(PatchType type, PatchVersion version) {
        super(type, version);
    }
    public v3_8_2_AccessToken_TTL_Index(){
    }
    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        String collectionName = "AccessToken";
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        long buffer = 86400L;
        long ttlValue = accessTokenTtl + buffer;

        Index index = new Index().on("last_updated", Sort.Direction.DESC).expire(ttlValue, TimeUnit.SECONDS).background();
        mongoTemplate.indexOps(collectionName).ensureIndex(index);
    }
}
