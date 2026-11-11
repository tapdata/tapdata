package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SensitiveDataMasker;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 审计存量凭证脱敏与引擎恢复状态修复；重复执行安全，不重放历史通知。 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.25-3")
public class V4_25_3_AuditAndRecovery extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_25_3_AuditAndRecovery.class);
    private static final int BATCH_SIZE = 500;
    private static final List<String> FIELDS = List.of("parameter1", "parameter2", "parameter3", "desc", "url");

    public V4_25_3_AuditAndRecovery(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        migrate(SpringContextHelper.getBean(MongoTemplate.class));
    }

    public static void migrate(MongoTemplate mongo) {
        ObjectId lastId = null;
        long masked = 0;
        while (true) {
            Query query = lastId == null ? new Query() : Query.query(Criteria.where("_id").gt(lastId));
            query.with(Sort.by("_id")).limit(BATCH_SIZE);
            query.fields().include("_id").include("modular");
            FIELDS.forEach(field -> query.fields().include(field));
            List<Document> documents = mongo.find(query, Document.class, "UserLogs");
            if (documents.isEmpty()) {
                break;
            }
            lastId = documents.get(documents.size() - 1).getObjectId("_id");
            List<ObjectId> ids = new ArrayList<>();
            documents.forEach(document -> ids.add(document.getObjectId("_id")));
            // 并发节点可能先完成同一批清洗；冲突时重新读取新值，避免覆盖并发写入。
            for (int attempt = 0; ; attempt++) {
                BulkOperations bulk = mongo.bulkOps(BulkOperations.BulkMode.UNORDERED, "UserLogs");
                int updates = 0;
                for (Document document : documents) {
                    Update update = new Update();
                    Criteria unchanged = Criteria.where("_id").is(document.getObjectId("_id"));
                    boolean changed = false;
                    for (String field : FIELDS) {
                        Object value = document.get(field);
                        if (!(value instanceof String)) {
                            continue;
                        }
                        String safe = SensitiveDataMasker.maskAuditField(document.getString("modular"), field, (String) value);
                        if (!Objects.equals(value, safe)) {
                            update.set(field, safe);
                            unchanged.and(field).is(value);
                            changed = true;
                        }
                    }
                    if (changed) {
                        bulk.updateOne(Query.query(unchanged), update);
                        updates++;
                    }
                }
                if (updates == 0) {
                    break;
                }
                com.mongodb.bulk.BulkWriteResult result = bulk.execute();
                masked += result.getModifiedCount();
                if (result.getMatchedCount() == updates) {
                    break;
                }
                if (attempt >= 4) {
                    throw new IllegalStateException("Audit records repeatedly changed during sanitization");
                }
                Query retry = Query.query(Criteria.where("_id").in(ids));
                retry.fields().include("_id").include("modular");
                FIELDS.forEach(field -> retry.fields().include(field));
                documents = mongo.find(retry, Document.class, "UserLogs");
            }
        }
        // 只修复明确错误的引擎上线事件；不触碰已关闭事件，也不刷新lastNotifyTime。
        Document recoveryTime = new Document("$ifNull", List.of("$recoveryTime",
                new Document("$ifNull", List.of("$lastOccurrenceTime",
                        new Document("$ifNull", List.of("$firstOccurrenceTime", "$$REMOVE"))))));
        long recovered = mongo.getCollection("AlarmInfo").updateMany(
                new Document("metric", "ENGINE_ONLINE").append("level", "RECOVERY").append("status", "ING"),
                List.of(new Document("$set", new Document("status", "RECOVER").append("recoveryTime", recoveryTime))))
                .getModifiedCount();
        logger.info("Audit and recovery patch completed: masked={}, recovered={}", masked, recovered);
    }
}
