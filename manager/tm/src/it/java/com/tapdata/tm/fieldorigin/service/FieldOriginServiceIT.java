package com.tapdata.tm.fieldorigin.service;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.fieldorigin.builder.NodeSchema;
import com.tapdata.tm.fieldorigin.builder.NodeSchemaProvider;
import com.tapdata.tm.fieldorigin.builder.OriginGraphNode;
import com.tapdata.tm.fieldorigin.dto.FieldOriginDto;
import com.tapdata.tm.fieldorigin.dto.FieldOriginQueryParam;
import com.tapdata.tm.fieldorigin.dto.FieldOriginQueryResult;
import com.tapdata.tm.fieldorigin.dto.FieldOriginSnapshot;
import com.tapdata.tm.fieldorigin.dto.FieldOriginVersionDto;
import com.tapdata.tm.fieldorigin.dto.FieldOriginVersionInfo;
import com.tapdata.tm.fieldorigin.entity.FieldOriginEntity;
import com.tapdata.tm.fieldorigin.entity.FieldOriginVersionEntity;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** L2: real field origin builder and versioning service; task models and Mongo storage are boundaries. */
@Timeout(10)
class FieldOriginServiceIT {
    private static final String SRC = "srcConn";
    private static final String TGT = "tgtConn";
    private InMemoryStore store;
    private StubLoader loader;
    private FieldOriginService service;
    private TaskDto task;
    private String taskId;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        loader = new StubLoader();
        service = new FieldOriginService(store, loader);
        task = new TaskDto();
        task.setId(new ObjectId());
        task.setCustomId("customer");
        task.setUserId("user");
        taskId = task.getId().toHexString();
        playerMdm(false);
    }

    static Field field(String id, String name, String originalName) {
        Field field = new Field();
        field.setId(id);
        field.setFieldName(name);
        field.setOriginalFieldName(originalName);
        return field;
    }

    static String sid(String table, String field) {
        return SRC + "_" + table + "_" + field;
    }

    static MergeTableProperties property(String id, String targetPath, MergeTableProperties.MergeType type, MergeTableProperties... children) {
        MergeTableProperties property = new MergeTableProperties();
        property.setId(id);
        property.setTargetPath(targetPath);
        property.setMergeType(type);
        property.setChildren(new ArrayList<>(List.of(children)));
        return property;
    }

    /** Player MDM: Player + address(+country) + phones array; optionally the address gets a renamed zip field. */
    void playerMdm(boolean withZip) {
        loader.graph.clear();
        loader.schemas.clear();
        loader.source("player", "Player", "id", "name");
        loader.source("addr", "PlayerAddress", "city", "zip");
        loader.source("country", "Country", "name");
        loader.source("phone", "PlayerPhone", "number");
        OriginGraphNode merge = OriginGraphNode.processor("m", "merge_table_processor").from("player", "addr", "country", "phone");
        merge.setMergeProperties(List.of(property("player", null, MergeTableProperties.MergeType.updateOrInsert,
                property("addr", "address", MergeTableProperties.MergeType.updateWrite,
                        property("country", "address.country", MergeTableProperties.MergeType.updateWrite)),
                property("phone", "phones", MergeTableProperties.MergeType.updateIntoArray))));
        loader.graph.add(merge);
        loader.graph.add(OriginGraphNode.data("t", "table", TGT).from("m"));
        List<Field> fields = new ArrayList<>(List.of(
                field(sid("Player", "id"), "id", "id"),
                field(sid("Player", "name"), "name", "name"),
                field("m_PlayerAddress_address", "address", "address"),
                field(sid("PlayerAddress", "city"), "address.city", "city"),
                field(sid("Country", "name"), "address.country.name", "name"),
                field("m_PlayerPhone_phones", "phones", "phones"),
                field(sid("PlayerPhone", "number"), "phones.number", "number")));
        if (withZip) {
            fields.add(field(sid("PlayerAddress", "zip"), "address.postCode", "zip"));
        }
        loader.schemas.put("t", List.of(new NodeSchema("t", "PlayerMDM", fields)));
    }

    FieldOriginQueryParam param(String... paths) {
        FieldOriginQueryParam param = new FieldOriginQueryParam();
        param.setTaskId(taskId);
        param.setFieldPaths(List.of(paths));
        return param;
    }

    @Test
    @DisplayName("first build activates version 1, an unchanged rebuild creates no new version")
    void unchangedConfigDoesNotCreateVersion() {
        FieldOriginVersionDto first = service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);
        FieldOriginVersionDto second = service.rebuild(task, FieldOriginService.TRIGGER_MANUAL);

        assertThat(first.getVersion()).isEqualTo(1L);
        assertThat(first.getStatus()).isEqualTo(FieldOriginVersionEntity.STATUS_SUCCESS);
        assertThat(first.getActive()).isTrue();
        assertThat(first.getStats().getTotalFields()).isEqualTo(7);
        assertThat(first.getStats().getContainerFields()).isEqualTo(2);
        assertThat(first.getStats().getUnknownFields()).isZero();
        assertThat(second.getVersion()).isEqualTo(1L);
        assertThat(store.versions).hasSize(1);
        assertThat(store.records).allMatch(r -> "customer".equals(r.getCustomId()) && taskId.equals(r.getTaskId()));
    }

    @Test
    @DisplayName("config change creates and activates a new version, the old version stays queryable")
    void configChangeCreatesNewVersion() {
        service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);
        playerMdm(true);

        FieldOriginVersionDto second = service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);

        assertThat(second.getVersion()).isEqualTo(2L);
        assertThat(store.version(1).getActive()).isFalse();
        assertThat(store.version(2).getActive()).isTrue();
        FieldOriginQueryResult latest = service.query(param("address.postCode"));
        assertThat(latest.getVersion()).isEqualTo(2L);
        FieldOriginDto zip = latest.getOrigins().get("address.postCode").get(0);
        assertThat(zip.getOriginType()).isEqualTo("RENAMED");
        assertThat(zip.getSourceField()).isEqualTo("zip");
        FieldOriginQueryParam old = param("address.postCode", "address.city");
        old.setVersion(1L);
        FieldOriginQueryResult previous = service.query(old);
        assertThat(previous.getVersion()).isEqualTo(1L);
        assertThat(previous.getNotFound()).containsExactly("address.postCode");
        assertThat(previous.getOrigins()).containsKey("address.city");
    }

    @Test
    @DisplayName("failed save keeps the previous active version queryable and reports the failed build")
    void failedBuildKeepsPreviousVersion() {
        service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);
        playerMdm(true);
        store.failInsert = true;

        FieldOriginVersionDto failed = service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);

        assertThat(failed.getStatus()).isEqualTo(FieldOriginVersionEntity.STATUS_FAILED);
        assertThat(failed.getError()).contains("storage down");
        assertThat(store.records).noneMatch(r -> r.getVersion() == 2L);
        FieldOriginVersionInfo info = service.version(taskId);
        assertThat(info.getActive().getVersion()).isEqualTo(1L);
        assertThat(info.getLatestBuild().getVersion()).isEqualTo(2L);
        assertThat(info.getLatestBuild().getStatus()).isEqualTo(FieldOriginVersionEntity.STATUS_FAILED);
        assertThat(service.query(param("address.city")).getVersion()).isEqualTo(1L);
        FieldOriginQueryParam failedVersion = param("address.city");
        failedVersion.setVersion(2L);
        assertThat(service.query(failedVersion).getNotFound()).containsExactly("address.city");
    }

    @Test
    @DisplayName("builder failure is recorded without touching the active version")
    void builderFailureIsRecorded() {
        service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);
        loader.failSchemas = true;

        FieldOriginVersionDto failed = service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);

        assertThat(failed.getStatus()).isEqualTo(FieldOriginVersionEntity.STATUS_FAILED);
        assertThat(service.version(taskId).getActive().getVersion()).isEqualTo(1L);
    }

    @Test
    @DisplayName("batch query normalizes array indexes and container paths return their children")
    void batchQueryWithPrefix() {
        service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);

        FieldOriginQueryResult result = service.query(param("phones[0].number", "phones.1.number", "address", "name", "missing"));

        assertThat(result.getOrigins().get("phones[0].number")).singleElement()
                .satisfies(o -> {
                    assertThat(o.getSourceTable()).isEqualTo("PlayerPhone");
                    assertThat(o.getTargetPath()).isEqualTo("phones");
                    assertThat(o.getNormalizedFieldPath()).isEqualTo("phones[].number");
                });
        assertThat(result.getOrigins().get("phones.1.number")).hasSize(1);
        assertThat(result.getOrigins().get("address")).extracting(FieldOriginDto::getFieldPath)
                .containsExactlyInAnyOrder("address", "address.city", "address.country.name");
        assertThat(result.getOrigins().get("address")).filteredOn(o -> o.getFieldPath().equals("address.country.name"))
                .singleElement().satisfies(o -> {
                    assertThat(o.getSourceTable()).isEqualTo("Country");
                    assertThat(o.getMergePropertyId()).isEqualTo("country");
                });
        assertThat(result.getOrigins().get("name")).singleElement().extracting(FieldOriginDto::getSourceTable).isEqualTo("Player");
        assertThat(result.getNotFound()).containsExactly("missing");
    }

    @Test
    @DisplayName("migrate task builds every target table and snapshot filters by table")
    void migrateTaskSnapshot() {
        loader.graph.clear();
        loader.schemas.clear();
        loader.graph.add(OriginGraphNode.data("src", "database", SRC));
        loader.schemas.put("src", List.of(
                new NodeSchema("src", "orders", List.of(field(sid("orders", "id"), "id", "id"))),
                new NodeSchema("src", "users", List.of(field(sid("users", "id"), "id", "id")))));
        loader.graph.add(OriginGraphNode.processor("rename", "table_rename_processor").from("src"));
        loader.graph.add(OriginGraphNode.data("tgt", "database", TGT).from("rename"));
        loader.schemas.put("tgt", List.of(
                new NodeSchema("tgt", "ORDERS", List.of(field(sid("orders", "id"), "id", "id"))),
                new NodeSchema("tgt", "USERS", List.of(field(sid("users", "id"), "id", "id")))));

        FieldOriginVersionDto version = service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);
        FieldOriginSnapshot all = service.snapshot(taskId, null, null, null);
        FieldOriginSnapshot users = service.snapshot(taskId, null, TGT, "USERS");

        assertThat(version.getStats().getTargetTables()).isEqualTo(2);
        assertThat(all.getOrigins()).hasSize(2);
        assertThat(users.getOrigins()).singleElement().satisfies(o -> {
            assertThat(o.getSourceTable()).isEqualTo("users");
            assertThat(o.getTargetTable()).isEqualTo("USERS");
        });
        FieldOriginQueryParam param = param("id");
        param.setTargetTable("ORDERS");
        assertThat(service.query(param).getOrigins().get("id")).singleElement()
                .extracting(FieldOriginDto::getSourceTable).isEqualTo("orders");
    }

    @Test
    @DisplayName("old versions are pruned but the active and newest builds are kept")
    void oldVersionsArePruned() {
        ReflectionTestUtils.setField(service, "keepVersions", 1);
        for (int i = 0; i < 4; i++) {
            loader.schemas.put("t", List.of(new NodeSchema("t", "PlayerMDM", List.of(field(sid("Player", "id"), "id" + i, "id")))));
            service.rebuild(task, FieldOriginService.TRIGGER_TRANSFORM);
        }

        assertThat(store.versions).extracting(FieldOriginVersionEntity::getVersion).containsExactlyInAnyOrder(3L, 4L);
        assertThat(store.records).extracting(FieldOriginEntity::getVersion).containsOnly(3L, 4L);
        assertThat(store.version(4).getActive()).isTrue();
    }

    @Test
    @DisplayName("schema transform triggers an asynchronous rebuild")
    void asyncRebuildAfterTransform() throws InterruptedException {
        TaskService tasks = mock(TaskService.class);
        when(tasks.findByTaskId(eq(task.getId()), any(String[].class))).thenReturn(task);
        ReflectionTestUtils.setField(service, "taskService", tasks);

        service.rebuildAsync(taskId);

        long deadline = System.currentTimeMillis() + 5000;
        while (store.findActive(taskId) == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(store.findActive(taskId)).isNotNull();
        assertThat(store.findActive(taskId).getTrigger()).isEqualTo(FieldOriginService.TRIGGER_TRANSFORM);
    }

    static class StubLoader extends FieldOriginTaskLoader {
        final List<OriginGraphNode> graph = new ArrayList<>();
        final Map<String, List<NodeSchema>> schemas = new HashMap<>();
        boolean failSchemas;

        StubLoader() {
            super(null);
        }

        void source(String id, String table, String... fields) {
            graph.add(OriginGraphNode.data(id, "table", SRC));
            List<Field> list = new ArrayList<>();
            for (String f : fields) {
                list.add(field(sid(table, f), f, f));
            }
            schemas.put(id, List.of(new NodeSchema(id, table, list)));
        }

        @Override
        public List<OriginGraphNode> graph(TaskDto task) {
            return graph;
        }

        @Override
        public NodeSchemaProvider schemaProvider(String taskId) {
            return nodeId -> {
                if (failSchemas) {
                    throw new IllegalStateException("metadata unavailable");
                }
                return schemas.getOrDefault(nodeId, List.of());
            };
        }
    }

    static class InMemoryStore implements FieldOriginStore {
        final List<FieldOriginVersionEntity> versions = new CopyOnWriteArrayList<>();
        final List<FieldOriginEntity> records = new CopyOnWriteArrayList<>();
        boolean failInsert;

        FieldOriginVersionEntity version(long number) {
            return versions.stream().filter(v -> v.getVersion() == number).findFirst().orElse(null);
        }

        @Override
        public long nextVersion(String taskId) {
            return versions.stream().filter(v -> v.getTaskId().equals(taskId)).mapToLong(FieldOriginVersionEntity::getVersion).max().orElse(0L) + 1;
        }

        @Override
        public FieldOriginVersionEntity findActive(String taskId) {
            return versions.stream().filter(v -> v.getTaskId().equals(taskId) && Boolean.TRUE.equals(v.getActive())).findFirst().orElse(null);
        }

        @Override
        public FieldOriginVersionEntity findVersion(String taskId, long version) {
            return versions.stream().filter(v -> v.getTaskId().equals(taskId) && v.getVersion() == version).findFirst().orElse(null);
        }

        @Override
        public FieldOriginVersionEntity findLatestBuild(String taskId) {
            return versions.stream().filter(v -> v.getTaskId().equals(taskId))
                    .max(Comparator.comparing(FieldOriginVersionEntity::getVersion)).orElse(null);
        }

        @Override
        public void saveVersion(FieldOriginVersionEntity version) {
            if (!versions.contains(version)) {
                versions.removeIf(v -> v.getTaskId().equals(version.getTaskId()) && Objects.equals(v.getVersion(), version.getVersion()));
                versions.add(version);
            }
        }

        @Override
        public void insertRecords(List<FieldOriginEntity> list) {
            if (failInsert) {
                records.addAll(list.subList(0, 1));
                throw new IllegalStateException("storage down");
            }
            records.addAll(list);
        }

        @Override
        public void deleteRecords(String taskId, long version) {
            records.removeIf(r -> r.getTaskId().equals(taskId) && r.getVersion() == version);
        }

        @Override
        public void activate(String taskId, long version) {
            versions.stream().filter(v -> v.getTaskId().equals(taskId)).forEach(v -> v.setActive(v.getVersion() == version));
        }

        @Override
        public List<FieldOriginEntity> findRecords(String taskId, long version, String targetConnectionId, String targetTable, Collection<String> normalizedPaths) {
            return records.stream()
                    .filter(r -> r.getTaskId().equals(taskId) && r.getVersion() == version)
                    .filter(r -> null == targetConnectionId || targetConnectionId.equals(r.getTargetConnectionId()))
                    .filter(r -> null == targetTable || targetTable.equals(r.getTargetTable()))
                    .filter(r -> null == normalizedPaths || normalizedPaths.stream().anyMatch(p -> FieldOriginService.matches(r.getNormalizedFieldPath(), p)))
                    .toList();
        }

        @Override
        public List<FieldOriginVersionEntity> findPrunable(String taskId, int keep) {
            return FieldOriginVersions.prunable(versions.stream().filter(v -> v.getTaskId().equals(taskId)).toList(), keep);
        }

        @Override
        public void deleteVersion(String taskId, long version) {
            deleteRecords(taskId, version);
            versions.removeIf(v -> v.getTaskId().equals(taskId) && v.getVersion() == version);
        }
    }
}
