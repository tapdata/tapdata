package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** L1：真实 MongoDB 验证补丁持久化契约，测试数据库名称随机隔离。 */
@Timeout(60)
class AuditAndRecoveryPatchIT {
    private static String mongoUri() {
        String uri = System.getenv("TAPDATA_IT_MONGO_URI");
        assumeTrue(uri != null && !uri.isBlank(), "需要已授权临时 MongoDB：TAPDATA_IT_MONGO_URI");
        return uri;
    }

    @Test
    @DisplayName("历史修复：真实持久化脱敏和恢复状态，重复执行幂等且不重放通知")
    void should_repair_persisted_history_idempotently() {
        String uri = mongoUri();
        String database = "tap13209_" + UUID.randomUUID().toString().replace("-", "");
        try (MongoClient client = MongoClients.create(uri)) {
            MongoTemplate mongo = new MongoTemplate(client, database);
            try {
                ObjectId auditId = new ObjectId();
                ObjectId bareAccessCodeId = new ObjectId();
                ObjectId recoveryId = new ObjectId();
                ObjectId closedId = new ObjectId();
                Date occurred = new Date(1700000000000L);
                mongo.getCollection("UserLogs").insertOne(new Document("_id", auditId)
                        .append("parameter1", "/mcp?accessCode=synthetic-history")
                        .append("parameter3", "{\"password\":\"synthetic password\"}"));
                // 只存裸凭证的审计记录无法靠字段内容识别，必须依据 modular 判定
                mongo.getCollection("UserLogs").insertOne(new Document("_id", bareAccessCodeId)
                        .append("modular", "accessCode")
                        .append("parameter1", "synthetic-bare-code"));
                mongo.getCollection("AlarmInfo").insertMany(List.of(
                        new Document("_id", recoveryId).append("metric", "ENGINE_ONLINE")
                                .append("level", "RECOVERY").append("status", "ING")
                                .append("lastOccurrenceTime", occurred).append("lastNotifyTime", occurred),
                        new Document("_id", closedId).append("metric", "ENGINE_ONLINE")
                                .append("level", "RECOVERY").append("status", "CLOESE")));

                V4_25_3_AuditAndRecovery.migrate(mongo);
                Document audit = mongo.getCollection("UserLogs").find(new Document("_id", auditId)).first();
                Document bareAccessCode = mongo.getCollection("UserLogs").find(new Document("_id", bareAccessCodeId)).first();
                Document recovery = mongo.getCollection("AlarmInfo").find(new Document("_id", recoveryId)).first();
                assertEquals("/mcp?accessCode=***", audit.getString("parameter1"));
                assertFalse(audit.toJson().contains("synthetic"));
                assertEquals("***", bareAccessCode.getString("parameter1"));
                assertEquals("RECOVER", recovery.getString("status"));
                assertEquals(occurred, recovery.getDate("recoveryTime"));
                assertEquals(occurred, recovery.getDate("lastNotifyTime"));
                assertEquals("CLOESE", mongo.getCollection("AlarmInfo")
                        .find(new Document("_id", closedId)).first().getString("status"));

                V4_25_3_AuditAndRecovery.migrate(mongo);
                assertEquals(audit, mongo.getCollection("UserLogs").find(new Document("_id", auditId)).first());
                assertEquals(recovery, mongo.getCollection("AlarmInfo").find(new Document("_id", recoveryId)).first());
            } finally {
                client.getDatabase(database).drop();
            }
        }
    }

    @Test
    @DisplayName("历史修复：多节点同时启动时并发清洗不失败且全部脱敏")
    void should_sanitize_concurrently_from_multiple_nodes() throws Exception {
        String uri = mongoUri();
        String database = "tap13209_" + UUID.randomUUID().toString().replace("-", "");
        try (MongoClient client = MongoClients.create(uri)) {
            MongoTemplate mongo = new MongoTemplate(client, database);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                List<Document> documents = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    documents.add(new Document()
                            .append("parameter1", "token=synthetic-token-" + i)
                            .append("url", "/api?accessCode=synthetic-code-" + i));
                }
                mongo.getCollection("UserLogs").insertMany(documents);

                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    futures.add(pool.submit(() -> {
                        start.await();
                        V4_25_3_AuditAndRecovery.migrate(mongo);
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> future : futures) {
                    future.get(50, TimeUnit.SECONDS);
                }

                for (Document stored : mongo.getCollection("UserLogs").find()) {
                    assertFalse(stored.toJson().contains("synthetic"), "未脱敏记录：" + stored.get("_id"));
                }
            } finally {
                pool.shutdownNow();
                client.getDatabase(database).drop();
            }
        }
    }
}
