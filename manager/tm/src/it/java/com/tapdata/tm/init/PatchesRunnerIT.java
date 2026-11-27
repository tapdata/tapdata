package com.tapdata.tm.init;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.daas.V4_25_3_DashboardMetricsIndexes;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.verison.repository.VersionRepository;
import com.tapdata.tm.verison.service.VersionService;
import io.tapdata.utils.AppType;
import io.tapdata.utils.UnitTestUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

/** L1：真实 MongoDB 验证 4.25-2/4.25-3 存量升级、用户配置保留与 executedPatches 防跳过/防重跑契约。 */
@Timeout(60)
class PatchesRunnerIT {

    private static final String ADMIN_ROLE_OID = "5b9a0a383fcba02649524bf1";
    private static final String SCRIPT_4254_ID = "script:4.25-4.json";
    private static final String DASHBOARD_PATCH_ID = "java:" + V4_25_3_DashboardMetricsIndexes.class.getName();

    private static String mongoUri() {
        String uri = System.getenv("TAPDATA_IT_MONGO_URI");
        if (uri == null || uri.isBlank()) {
            uri = System.getenv("TAPDATA_TEST_MONGO_URI");
        }
        Assumptions.assumeTrue(uri != null && !uri.isBlank(),
                "需要隔离 MongoDB 连接环境变量 TAPDATA_IT_MONGO_URI 或 TAPDATA_TEST_MONGO_URI");
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE")),
                "TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE=true is required because the test creates and drops a random database");
        return uri;
    }

    @Test
    @DisplayName("4.25-2 存量库升级到 4.25-5：自动补齐脱敏、导出权限、管理员授权、90天保留期及审计与仪表盘索引")
    void shouldRepairMissingAuditSeedAndIndexesWhenUpgradingFrom4_25_2() throws Exception {
        withIsolatedMongo((uri, defaultTemplate, obsTemplate) -> {
            ObjectId userLogId = new ObjectId();
            defaultTemplate.getCollection("Version").insertOne(new Document()
                    .append("type", "daas_script_version")
                    .append("version", "4.25-2"));
            defaultTemplate.getCollection("UserLogs").insertOne(new Document()
                    .append("_id", userLogId)
                    .append("parameter1", "/mcp?accessCode=synthetic-secret"));

            prepareDashboardCollections(defaultTemplate, obsTemplate);

            PatchesRunner runner = buildRunner(uri, defaultTemplate);
            try (MockedStatic<InitLogMap> initLogMap = mockStatic(InitLogMap.class)) {
                initLogMap.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                runner.run(null);
            }

            awaitIndex(obsTemplate, MeasurementEntity.COLLECTION_NAME, "idx_task_dashboard_dirty");
            awaitIndex(defaultTemplate, MongoUtils.getCollectionName(ApiMetricsRaw.class), "api_metrics_node_trend");

            // 1. Version 水位线与 executedPatches 精确落库
            Document versionDoc = defaultTemplate.getCollection("Version")
                    .find(new Document("type", "daas_script_version")).first();
            assertNotNull(versionDoc);
            assertEquals("4.25-5", versionDoc.getString("version"));
            List<String> executed = versionDoc.getList("executedPatches", String.class);
            assertNotNull(executed);
            assertTrue(executed.contains(SCRIPT_4254_ID));
            assertTrue(executed.contains(DASHBOARD_PATCH_ID));

            // 2. V4_25_3_AuditAndRecovery 脱敏生效
            Document sanitizedLog = defaultTemplate.getCollection("UserLogs")
                    .find(new Document("_id", userLogId)).first();
            assertNotNull(sanitizedLog);
            assertEquals("/mcp?accessCode=***", sanitizedLog.getString("parameter1"));

            // 3. 4.25-4.json Permission / RoleMapping / Settings / Indexes 完整落库
            assertNotNull(defaultTemplate.getCollection("Permission")
                    .find(new Document("name", "v2_audit_log_export")).first());
            assertNotNull(defaultTemplate.getCollection("Permission")
                    .find(new Document("name", "v2_audit_log")).first());
            assertNotNull(defaultTemplate.getCollection("Permission")
                    .find(new Document("name", "v2_audit_log_list")).first());
            Document exportRoleMapping = defaultTemplate.getCollection("RoleMapping").find(new Document()
                    .append("principalId", "v2_audit_log_export")
                    .append("principalType", "PERMISSION")
                    .append("roleId", new ObjectId(ADMIN_ROLE_OID))).first();
            assertNotNull(exportRoleMapping);

            Document retentionSetting = defaultTemplate.getCollection("Settings")
                    .find(new Document("key", "audit.log.retention.days")).first();
            assertNotNull(retentionSetting);
            assertEquals(90, ((Number) retentionSetting.get("value")).intValue());
            assertEquals(0, ((Number) retentionSetting.get("last_update")).intValue());
            assertEquals("", retentionSetting.getString("last_update_by"));

            Set<String> userLogIndexes = indexNames(defaultTemplate, "UserLogs");
            assertTrue(userLogIndexes.containsAll(List.of(
                    "eventId_1", "operationId_1_timestamp_1", "type_1_timestamp_-1_eventId_-1")));
            assertTrue(indexNames(defaultTemplate, "AuditOperations").contains("createdAt_ttl"));
            assertTrue(indexNames(obsTemplate, MeasurementEntity.COLLECTION_NAME).contains("measurement_date_ttl"));
        });
    }

    @Test
    @DisplayName("4.25-3 存量库升级到 4.25-5：保留用户已修改的保留期设置与更新时间，同时补齐缺失权限与索引")
    void shouldPreserveExistingRetentionSettingWhenUpgradingFrom4_25_3() throws Exception {
        withIsolatedMongo((uri, defaultTemplate, obsTemplate) -> {
            defaultTemplate.getCollection("Version").insertOne(new Document()
                    .append("type", "daas_script_version")
                    .append("version", "4.25-3"));
            defaultTemplate.getCollection("Settings").insertOne(new Document()
                    .append("key", "audit.log.retention.days")
                    .append("category", "Log")
                    .append("value", 180)
                    .append("last_update", 1700000001234L)
                    .append("last_update_by", "admin-custom"));
            prepareDashboardCollections(defaultTemplate, obsTemplate);

            PatchesRunner runner = buildRunner(uri, defaultTemplate);
            try (MockedStatic<InitLogMap> initLogMap = mockStatic(InitLogMap.class)) {
                initLogMap.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                runner.run(null);
            }

            awaitIndex(obsTemplate, MeasurementEntity.COLLECTION_NAME, "idx_task_dashboard_dirty");

            assertEquals(1L, defaultTemplate.getCollection("Settings")
                    .countDocuments(new Document("key", "audit.log.retention.days")));
            Document setting = defaultTemplate.getCollection("Settings")
                    .find(new Document("key", "audit.log.retention.days")).first();
            assertNotNull(setting);
            assertEquals(180, ((Number) setting.get("value")).intValue());
            assertEquals(1700000001234L, ((Number) setting.get("last_update")).longValue());
            assertEquals("admin-custom", setting.getString("last_update_by"));
            assertNotNull(defaultTemplate.getCollection("Permission")
                    .find(new Document("name", "v2_audit_log_export")).first());
            assertEquals("4.25-5", defaultTemplate.getCollection("Version")
                    .find(new Document("type", "daas_script_version")).first().getString("version"));
        });
    }

    @Test
    @DisplayName("已升级至 4.25-5 后重复启动不重跑，且同版本号新增补丁可精准补跑并写入 executedPatches")
    void shouldSkipAlreadyRecordedPatchesAndRunMissingSameVersionPatchOnRestart() throws Exception {
        withIsolatedMongo((uri, defaultTemplate, obsTemplate) -> {
            defaultTemplate.getCollection("Version").insertOne(new Document()
                    .append("type", "daas_script_version")
                    .append("version", "4.25-3"));
            prepareDashboardCollections(defaultTemplate, obsTemplate);

            PatchesRunner initialRunner = buildRunner(uri, defaultTemplate);
            try (MockedStatic<InitLogMap> initLogMap = mockStatic(InitLogMap.class)) {
                initLogMap.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                initialRunner.run(null);
            }
            awaitIndex(obsTemplate, MeasurementEntity.COLLECTION_NAME, "idx_task_dashboard_dirty");

            AtomicInteger legacyCount = new AtomicInteger();
            AtomicInteger concurrentCount = new AtomicInteger();
            String concurrentPatchId = "java:com.tapdata.tm.init.patches.daas.SimulatedConcurrent4255Patch";

            PatchesRunner restartNoopRunner = buildRunnerWithExtraPatch(
                    uri, defaultTemplate, legacyCount, concurrentCount, concurrentPatchId, false);
            try (MockedStatic<InitLogMap> initLogMap = mockStatic(InitLogMap.class)) {
                initLogMap.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                restartNoopRunner.run(null);
            }
            assertEquals(0, legacyCount.get());
            assertEquals(0, concurrentCount.get());

            PatchesRunner sameVersionNewPatchRunner = buildRunnerWithExtraPatch(
                    uri, defaultTemplate, legacyCount, concurrentCount, concurrentPatchId, true);
            try (MockedStatic<InitLogMap> initLogMap = mockStatic(InitLogMap.class)) {
                initLogMap.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                sameVersionNewPatchRunner.run(null);
            }
            assertEquals(0, legacyCount.get(), "Already recorded patches must not re-run");
            assertEquals(1, concurrentCount.get(), "Missing same-version patch must execute once");
            assertEquals(1L, defaultTemplate.getCollection("ConcurrentPatchAudit")
                    .countDocuments(new Document("marker", "concurrent-ok")));

            Document versionDoc = defaultTemplate.getCollection("Version")
                    .find(new Document("type", "daas_script_version")).first();
            assertNotNull(versionDoc);
            assertEquals("4.25-5", versionDoc.getString("version"));
            List<String> executed = versionDoc.getList("executedPatches", String.class);
            assertTrue(executed.containsAll(List.of(SCRIPT_4254_ID, DASHBOARD_PATCH_ID, concurrentPatchId)));
        });
    }

    @Test
    @DisplayName("已有等价审计索引重跑 4.25-4.json 时不触发 dropIndexes 或重建索引")
    void shouldNotDropOrRecreateEquivalentAuditIndexesWhenUpgradingFrom4_25_3() throws Exception {
        withIsolatedMongo((uri, defaultTemplate, obsTemplate) -> {
            defaultTemplate.getCollection("Version").insertOne(new Document()
                    .append("type", "daas_script_version")
                    .append("version", "4.25-3"));
            defaultTemplate.getCollection("UserLogs").createIndex(
                    new Document("eventId", 1),
                    new IndexOptions().name("eventId_1").sparse(true));
            defaultTemplate.getCollection("UserLogs").createIndex(
                    new Document("operationId", 1).append("timestamp", 1),
                    new IndexOptions().name("operationId_1_timestamp_1").sparse(true));
            defaultTemplate.getCollection("UserLogs").createIndex(
                    new Document("type", 1).append("timestamp", -1).append("eventId", -1),
                    new IndexOptions().name("type_1_timestamp_-1_eventId_-1"));
            defaultTemplate.getCollection("AuditOperations").createIndex(
                    new Document("createdAt", 1),
                    new IndexOptions().name("createdAt_ttl").expireAfter(604800L, TimeUnit.SECONDS));
            prepareDashboardCollections(defaultTemplate, obsTemplate);

            MongoTemplate spyTemplate = spy(defaultTemplate);
            List<String> executedCommands = new ArrayList<>();
            doAnswer(invocation -> {
                executedCommands.add(invocation.getArgument(0, String.class));
                return invocation.callRealMethod();
            }).when(spyTemplate).executeCommand(anyString());

            GenericApplicationContext spyContext = new GenericApplicationContext();
            org.springframework.context.ApplicationContext outerContext = SpringContextHelper.applicationContext;
            try {
                spyContext.registerBean("mongoTemplate", MongoTemplate.class, () -> spyTemplate);
                spyContext.registerBean("obsMongoTemplate", CompletableFuture.class,
                        () -> CompletableFuture.completedFuture(obsTemplate));
                spyContext.refresh();
                SpringContextHelper.applicationContext = spyContext;

                PatchesRunner runner = buildRunner(uri, spyTemplate);
                try (MockedStatic<InitLogMap> initLogMap = mockStatic(InitLogMap.class)) {
                    initLogMap.when(() -> InitLogMap.complete(any())).then(invocation -> null);
                    runner.run(null);
                }
                awaitIndex(obsTemplate, MeasurementEntity.COLLECTION_NAME, "idx_task_dashboard_dirty");
            } finally {
                SpringContextHelper.applicationContext = outerContext;
                if (spyContext.isActive()) {
                    spyContext.close();
                }
            }

            assertTrue(executedCommands.stream().noneMatch(cmd -> cmd.contains("dropIndexes")),
                    "Equivalent existing indexes must not be dropped: " + executedCommands);
            assertTrue(executedCommands.stream().noneMatch(cmd ->
                            cmd.contains("createIndexes") && (cmd.contains("\"UserLogs\"") || cmd.contains("\"AuditOperations\""))),
                    "Equivalent existing indexes must not be recreated: " + executedCommands);

            Set<String> userLogIndexes = indexNames(defaultTemplate, "UserLogs");
            assertTrue(userLogIndexes.containsAll(List.of(
                    "eventId_1", "operationId_1_timestamp_1", "type_1_timestamp_-1_eventId_-1")));
            Document ttlIndex = defaultTemplate.getCollection("AuditOperations").listIndexes().into(new ArrayList<>()).stream()
                    .filter(doc -> "createdAt_ttl".equals(doc.getString("name")))
                    .findFirst()
                    .orElse(null);
            assertNotNull(ttlIndex);
            assertEquals(604800L, ((Number) ttlIndex.get("expireAfterSeconds")).longValue());
            assertNotNull(defaultTemplate.getCollection("Permission")
                    .find(new Document("name", "v2_audit_log_export")).first());
            Document versionDoc = defaultTemplate.getCollection("Version")
                    .find(new Document("type", "daas_script_version")).first();
            assertNotNull(versionDoc);
            assertEquals("4.25-5", versionDoc.getString("version"));
            assertTrue(versionDoc.getList("executedPatches", String.class).contains(SCRIPT_4254_ID));
        });
    }

    private PatchesRunner buildRunner(String uri, MongoTemplate defaultTemplate) {
        PatchesRunner runner = new PatchesRunner();
        VersionRepository repository = new VersionRepository(defaultTemplate);
        VersionService versionService = new VersionService(repository);
        UnitTestUtils.injectField(PatchesRunner.class, runner, "versionService", versionService);
        UnitTestUtils.injectField(PatchesRunner.class, runner, "productList", Collections.singletonList("idaas"));
        UnitTestUtils.injectField(PatchesRunner.class, runner, "mongodbUri", uri);
        return runner;
    }

    private PatchesRunner buildRunnerWithExtraPatch(
            String uri,
            MongoTemplate defaultTemplate,
            AtomicInteger legacyCount,
            AtomicInteger concurrentCount,
            String concurrentPatchId,
            boolean includeConcurrentPatch) {
        PatchesRunner runner = new PatchesRunner() {
            @Override
            protected List<IPatch> scanPatches(AppType appType, PatchVersion appVersion, PatchVersion softVersion, IPatchScanner... scanners) {
                List<IPatch> list = new ArrayList<>();
                PatchType daasType = new PatchType(AppType.DAAS, PatchTypeEnums.Script);
                list.add(new AbsPatch(daasType, PatchVersion.valueOf("4.25-4")) {
                    @Override
                    public String patchId() {
                        return SCRIPT_4254_ID;
                    }

                    @Override
                    public void run() {
                        legacyCount.incrementAndGet();
                    }
                });
                list.add(new AbsPatch(daasType, PatchVersion.valueOf("4.25-5")) {
                    @Override
                    public String patchId() {
                        return DASHBOARD_PATCH_ID;
                    }

                    @Override
                    public void run() {
                        legacyCount.incrementAndGet();
                    }
                });
                if (includeConcurrentPatch) {
                    list.add(new AbsPatch(daasType, PatchVersion.valueOf("4.25-5")) {
                        @Override
                        public String patchId() {
                            return concurrentPatchId;
                        }

                        @Override
                        public void run() {
                            concurrentCount.incrementAndGet();
                            defaultTemplate.getCollection("ConcurrentPatchAudit")
                                    .insertOne(new Document("marker", "concurrent-ok"));
                        }
                    });
                }
                return list;
            }
        };
        VersionRepository repository = new VersionRepository(defaultTemplate);
        VersionService versionService = new VersionService(repository);
        UnitTestUtils.injectField(PatchesRunner.class, runner, "versionService", versionService);
        UnitTestUtils.injectField(PatchesRunner.class, runner, "productList", Collections.singletonList("idaas"));
        UnitTestUtils.injectField(PatchesRunner.class, runner, "mongodbUri", uri);
        return runner;
    }

    private void prepareDashboardCollections(MongoTemplate defaultTemplate, MongoTemplate obsTemplate) {
        defaultTemplate.createCollection(MongoUtils.getCollectionName(ApiMetricsRaw.class));
        obsTemplate.createCollection(MeasurementEntity.COLLECTION_NAME);
        obsTemplate.getCollection(MeasurementEntity.COLLECTION_NAME)
                .insertOne(new Document("date", new Date()).append("tags", new Document()).append("samples", List.of()));
        obsTemplate.getCollection(MeasurementEntity.COLLECTION_NAME)
                .createIndex(new Document("date", -1), new IndexOptions()
                        .name("measurement_date_ttl")
                        .expireAfter(7L, TimeUnit.DAYS));
    }

    private void awaitIndex(MongoTemplate template, String collection, String indexName) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000L;
        while (System.currentTimeMillis() < deadline) {
            if (indexNames(template, collection).contains(indexName)) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Index " + indexName + " on " + collection + " was not created within 15s");
    }

    private Set<String> indexNames(MongoTemplate template, String collection) {
        return template.getCollection(collection).listIndexes().into(new ArrayList<>()).stream()
                .map(doc -> doc.getString("name"))
                .collect(Collectors.toSet());
    }

    private void withIsolatedMongo(MongoCallback callback) throws Exception {
        String uri = mongoUri();
        String prefix = "tap13351_patches_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        MongoClient client = MongoClients.create(uri);
        MongoTemplate defaultTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, prefix + "_default"));
        MongoTemplate obsTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, prefix + "_obs"));
        GenericApplicationContext applicationContext = new GenericApplicationContext();
        ApplicationContext previousContext = SpringContextHelper.applicationContext;
        Throwable failure = null;
        try {
            applicationContext.registerBean("mongoTemplate", MongoTemplate.class, () -> defaultTemplate);
            applicationContext.registerBean("obsMongoTemplate", CompletableFuture.class,
                    () -> CompletableFuture.completedFuture(obsTemplate));
            applicationContext.refresh();
            SpringContextHelper.applicationContext = applicationContext;

            callback.run(uri, defaultTemplate, obsTemplate);
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            SpringContextHelper.applicationContext = previousContext;
            failure = attemptCleanup(failure, () -> {
                if (applicationContext.isActive()) {
                    applicationContext.close();
                }
            });
            failure = attemptCleanup(failure, () -> defaultTemplate.getDb().drop());
            failure = attemptCleanup(failure, () -> obsTemplate.getDb().drop());
            failure = attemptCleanup(failure, client::close);
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }

    private Throwable attemptCleanup(Throwable failure, Runnable cleanup) {
        try {
            cleanup.run();
        } catch (RuntimeException | Error cleanupFailure) {
            if (failure == null) {
                return cleanupFailure;
            }
            if (failure != cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
        return failure;
    }

    @FunctionalInterface
    private interface MongoCallback {
        void run(String uri, MongoTemplate defaultTemplate, MongoTemplate obsTemplate) throws Exception;
    }
}
