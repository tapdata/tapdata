package com.tapdata.tm.fieldorigin.builder;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldOriginBuilderTest {
    private static final String SRC = "srcConn";
    private static final String TGT = "tgtConn";

    private final List<OriginGraphNode> graph = new ArrayList<>();
    private final Map<String, List<NodeSchema>> schemas = new HashMap<>();

    static Field field(String id, String name) {
        return field(id, name, name);
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

    OriginGraphNode source(String id, String table, String... fields) {
        OriginGraphNode node = OriginGraphNode.data(id, "table", SRC);
        graph.add(node);
        List<Field> list = new ArrayList<>();
        for (String f : fields) {
            list.add(field(sid(table, f), f));
        }
        schemas.computeIfAbsent(id, k -> new ArrayList<>()).add(new NodeSchema(id, table, list));
        return node;
    }

    OriginGraphNode processor(String id, String type, String... from) {
        OriginGraphNode node = OriginGraphNode.processor(id, type).from(from);
        graph.add(node);
        return node;
    }

    OriginGraphNode target(String id, String table, List<Field> fields, String... from) {
        OriginGraphNode node = OriginGraphNode.data(id, "table", TGT).from(from);
        graph.add(node);
        schemas.computeIfAbsent(id, k -> new ArrayList<>()).add(new NodeSchema(id, table, fields));
        return node;
    }

    FieldOriginBuildResult build() {
        return new FieldOriginBuilder(graph, nodeId -> schemas.getOrDefault(nodeId, List.of())).build();
    }

    static List<FieldOriginRecord> of(FieldOriginBuildResult result, String path) {
        return result.getRecords().stream().filter(r -> r.getFieldPath().equals(path)).toList();
    }

    static FieldOriginRecord one(FieldOriginBuildResult result, String path) {
        List<FieldOriginRecord> records = of(result, path);
        assertEquals(1, records.size(), "records of " + path + ": " + records);
        return records.get(0);
    }

    @Nested
    class SyncTask {
        @Test
        void directFieldsAreMatchedById() {
            source("s", "Player", "id", "name");
            target("t", "PLAYER", List.of(field(sid("Player", "id"), "id"), field(sid("Player", "name"), "name")), "s");

            FieldOriginBuildResult result = build();

            FieldOriginRecord name = one(result, "name");
            assertEquals(OriginType.DIRECT, name.getOriginType());
            assertEquals(MatchMethod.ID, name.getMatchMethod());
            assertEquals("s", name.getSourceNodeId());
            assertEquals(SRC, name.getSourceConnectionId());
            assertEquals("Player", name.getSourceTable());
            assertEquals("name", name.getSourceField());
            assertEquals("t", name.getTargetNodeId());
            assertEquals(TGT, name.getTargetConnectionId());
            assertEquals("PLAYER", name.getTargetTable());
            assertEquals("name", name.getNormalizedFieldPath());
            assertNull(name.getMergeNodeId());
            assertEquals(2, result.getStats().getResolvedFields());
            assertEquals(1, result.getStats().getTargetTables());
        }

        @Test
        void renamedFieldKeepsItsIdAndPointsToTheOriginalField() {
            source("s", "Player", "name");
            processor("r", "field_rename_processor", "s");
            target("t", "PLAYER", List.of(field(sid("Player", "name"), "playerName", "name")), "r");

            FieldOriginRecord record = one(build(), "playerName");

            assertEquals(OriginType.RENAMED, record.getOriginType());
            assertEquals(MatchMethod.ID, record.getMatchMethod());
            assertEquals("name", record.getSourceField());
        }

        @Test
        void createdFieldsAreManualOrDerivedWithTheirNode() {
            source("s", "Player", "first", "last");
            processor("add", FieldOriginBuilder.FIELD_ADD_DEL_TYPE, "s");
            processor("calc", "field_calc_processor", "add");
            target("t", "PLAYER", List.of(
                    field("add_Player_flag", "flag"),
                    field("calc_Player_fullName", "fullName")), "calc");

            FieldOriginBuildResult result = build();

            FieldOriginRecord flag = one(result, "flag");
            assertEquals(OriginType.MANUAL, flag.getOriginType());
            assertEquals("add", flag.getTransformNodeId());
            FieldOriginRecord fullName = one(result, "fullName");
            assertEquals(OriginType.DERIVED, fullName.getOriginType());
            assertEquals("calc", fullName.getTransformNodeId());
            assertNull(fullName.getSourceTable());
            assertEquals(1, result.getStats().getManualFields());
            assertEquals(1, result.getStats().getDerivedFields());
        }

        @Test
        void jsOutputIsTracedByOriginalNameLikeDataTrace() {
            source("s", "Player", "name");
            processor("js", "js_processor", "s");
            target("t", "PLAYER", List.of(
                    field("js_Player_name", "name"),
                    field("js_Player_score", "score"),
                    field("unknownId", "mystery")), "js");

            FieldOriginBuildResult result = build();

            FieldOriginRecord name = one(result, "name");
            assertEquals(MatchMethod.NAME, name.getMatchMethod());
            assertEquals(OriginType.DIRECT, name.getOriginType());
            assertEquals("Player", name.getSourceTable());
            FieldOriginRecord score = one(result, "score");
            assertEquals(OriginType.DERIVED, score.getOriginType());
            assertEquals("js", score.getTransformNodeId());
            FieldOriginRecord mystery = one(result, "mystery");
            assertEquals(OriginType.UNKNOWN, mystery.getOriginType());
            assertEquals(MatchMethod.NONE, mystery.getMatchMethod());
            assertEquals(1, result.getStats().getUnknownFields());
            assertEquals(1, result.getStats().getNameMatchedFields());
            assertEquals(3, result.getStats().getTotalFields());
        }

        @Test
        void joinFieldsComeFromTheirOwnTables() {
            source("a", "Player", "id", "name");
            source("b", "Address", "id", "city");
            processor("join", "join_processor", "a", "b");
            target("t", "WIDE", List.of(field(sid("Player", "id"), "id"), field(sid("Address", "city"), "city")), "join");

            FieldOriginBuildResult result = build();

            assertEquals("a", one(result, "id").getSourceNodeId());
            assertEquals("b", one(result, "city").getSourceNodeId());
        }

        @Test
        void unionFieldHasEverySource() {
            source("a", "Shop1", "code");
            source("b", "Shop2", "code");
            processor("u", "union_processor", "a", "b");
            target("t", "SHOP", List.of(field(sid("Shop1", "code"), "code")), "u");

            FieldOriginBuildResult result = build();

            List<FieldOriginRecord> code = of(result, "code");
            assertEquals(2, code.size());
            assertEquals(MatchMethod.ID, code.stream().filter(r -> r.getSourceNodeId().equals("a")).findFirst().orElseThrow().getMatchMethod());
            assertEquals(MatchMethod.NAME, code.stream().filter(r -> r.getSourceNodeId().equals("b")).findFirst().orElseThrow().getMatchMethod());
            assertEquals(1, result.getStats().getMultiSourceFields());
        }

        @Test
        void deletedFieldsAreIgnored() {
            source("s", "Player", "id");
            Field deleted = field(sid("Player", "id"), "gone");
            deleted.setDeleted(true);
            target("t", "PLAYER", List.of(field(sid("Player", "id"), "id"), deleted), "s");

            FieldOriginBuildResult result = build();

            assertTrue(of(result, "gone").isEmpty());
            assertEquals(1, result.getStats().getTotalFields());
        }
    }

    @Nested
    class MergeTask {
        MergeTableProperties property(String id, String targetPath, MergeTableProperties.MergeType type, MergeTableProperties... children) {
            MergeTableProperties property = new MergeTableProperties();
            property.setId(id);
            property.setTargetPath(targetPath);
            property.setMergeType(type);
            property.setChildren(new ArrayList<>(List.of(children)));
            return property;
        }

        FieldOriginBuildResult buildPlayerMdm() {
            source("player", "Player", "id", "name");
            source("addr", "PlayerAddress", "id", "city");
            source("c1", "Country", "id", "name");
            source("ident", "PlayerIdentification", "no");
            source("c2", "Country", "id", "name");
            source("phone", "PlayerPhone", "number");
            OriginGraphNode merge = processor("m", FieldOriginBuilder.MERGE_NODE_TYPE, "player", "addr", "c1", "ident", "c2", "phone");
            merge.setMergeProperties(List.of(property("player", null, MergeTableProperties.MergeType.updateOrInsert,
                    property("addr", "address", MergeTableProperties.MergeType.updateWrite,
                            property("c1", "address.country", MergeTableProperties.MergeType.updateWrite)),
                    property("ident", "identification", MergeTableProperties.MergeType.updateWrite,
                            property("c2", "identification.country", MergeTableProperties.MergeType.updateWrite)),
                    property("phone", "phones", MergeTableProperties.MergeType.updateIntoArray))));
            target("t", "PlayerMDM", List.of(
                    field(sid("Player", "id"), "id"),
                    field(sid("Player", "name"), "name"),
                    field("m_PlayerAddress_address", "address"),
                    field(sid("PlayerAddress", "id"), "address.id", "id"),
                    field(sid("PlayerAddress", "city"), "address.city", "city"),
                    field("m_Country_address.country", "address.country"),
                    field(sid("Country", "name"), "address.country.name", "name"),
                    field(sid("PlayerIdentification", "no"), "identification.no", "no"),
                    field(sid("Country", "name"), "identification.country.name", "name"),
                    field("m_PlayerPhone_phones", "phones"),
                    field(sid("PlayerPhone", "number"), "phones.number", "number")), "m");
            return build();
        }

        @Test
        void nestedFieldsCarryMergePathAndSource() {
            FieldOriginBuildResult result = buildPlayerMdm();

            FieldOriginRecord city = one(result, "address.city");
            assertEquals(OriginType.DIRECT, city.getOriginType());
            assertEquals("addr", city.getSourceNodeId());
            assertEquals("PlayerAddress", city.getSourceTable());
            assertEquals("city", city.getSourceField());
            assertEquals("m", city.getMergeNodeId());
            assertEquals("addr", city.getMergePropertyId());
            assertEquals("address", city.getTargetPath());
            FieldOriginRecord name = one(result, "name");
            assertEquals("player", name.getMergePropertyId());
            assertEquals("", name.getTargetPath());
        }

        @Test
        void sameTableOnDifferentBranchesIsDistinguished() {
            FieldOriginBuildResult result = buildPlayerMdm();

            FieldOriginRecord addressCountry = one(result, "address.country.name");
            assertEquals("c1", addressCountry.getSourceNodeId());
            assertEquals("address.country", addressCountry.getTargetPath());
            FieldOriginRecord identCountry = one(result, "identification.country.name");
            assertEquals("c2", identCountry.getSourceNodeId());
            assertEquals("identification.country", identCountry.getTargetPath());
            assertEquals("Country", identCountry.getSourceTable());
        }

        @Test
        void sameFieldNameOfDifferentTablesIsNotMixed() {
            FieldOriginBuildResult result = buildPlayerMdm();

            assertEquals("player", one(result, "id").getSourceNodeId());
            assertEquals("addr", one(result, "address.id").getSourceNodeId());
        }

        @Test
        void containersAndArraysAreNormalized() {
            FieldOriginBuildResult result = buildPlayerMdm();

            FieldOriginRecord address = one(result, "address");
            assertEquals(OriginType.CONTAINER, address.getOriginType());
            assertEquals("addr", address.getMergePropertyId());
            assertEquals(OriginType.CONTAINER, one(result, "phones").getOriginType());
            FieldOriginRecord number = one(result, "phones.number");
            assertEquals("phones[].number", number.getNormalizedFieldPath());
            assertEquals("phone", number.getSourceNodeId());
            assertEquals(3, result.getStats().getContainerFields());
            assertEquals(0, result.getStats().getUnknownFields());
        }
    }

    @Nested
    class MigrateTask {
        @Test
        void everyTargetTableIsBuiltAndNameFallbackPrefersTheSameSourceTable() {
            OriginGraphNode db = OriginGraphNode.data("src", "database", SRC);
            graph.add(db);
            schemas.put("src", List.of(
                    new NodeSchema("src", "orders", List.of(field(sid("orders", "id"), "id"), field(sid("orders", "amount"), "amount"))),
                    new NodeSchema("src", "users", List.of(field(sid("users", "id"), "id"), field(sid("users", "name"), "name")))));
            processor("rename", "table_rename_processor", "src");
            processor("js", "migrate_js_processor", "rename");
            OriginGraphNode tgt = OriginGraphNode.data("tgt", "database", TGT).from("js");
            graph.add(tgt);
            schemas.put("tgt", List.of(
                    new NodeSchema("tgt", "ORDERS", List.of(field(sid("orders", "id"), "id"), field(sid("orders", "amount"), "amount"))),
                    new NodeSchema("tgt", "USERS", List.of(
                            field(sid("users", "name"), "userName", "name"),
                            field("js_USERS_id", "id")))));

            FieldOriginBuildResult result = build();

            assertEquals(2, result.getStats().getTargetTables());
            FieldOriginRecord userName = result.getRecords().stream()
                    .filter(r -> r.getTargetTable().equals("USERS") && r.getFieldPath().equals("userName")).findFirst().orElseThrow();
            assertEquals(OriginType.RENAMED, userName.getOriginType());
            assertEquals("users", userName.getSourceTable());
            List<FieldOriginRecord> usersId = result.getRecords().stream()
                    .filter(r -> r.getTargetTable().equals("USERS") && r.getFieldPath().equals("id")).toList();
            assertEquals(1, usersId.size());
            assertEquals("users", usersId.get(0).getSourceTable());
            assertEquals(MatchMethod.NAME, usersId.get(0).getMatchMethod());
        }
    }

    @Nested
    class Helpers {
        @Test
        void queryPathsAreNormalized() {
            assertEquals("phones[].number", FieldOriginBuilder.normalizeQueryPath("phones[0].number"));
            assertEquals("phones[].number", FieldOriginBuilder.normalizeQueryPath("phones.12.number"));
            assertEquals("a[].b[].c", FieldOriginBuilder.normalizeQueryPath("a[1].b.2.c"));
            assertEquals("phones[]", FieldOriginBuilder.normalizeQueryPath("phones.0"));
            assertEquals("address.city", FieldOriginBuilder.normalizeQueryPath(" address.city "));
            assertNull(FieldOriginBuilder.normalizeQueryPath(null));
        }

        @Test
        void configHashIgnoresOrderButNotContent() {
            source("s", "Player", "id", "name");
            target("t", "PLAYER", List.of(field(sid("Player", "id"), "id"), field(sid("Player", "name"), "name")), "s");
            FieldOriginBuildResult result = build();
            List<FieldOriginRecord> reversed = new ArrayList<>(result.getRecords());
            java.util.Collections.reverse(reversed);

            assertEquals(result.getConfigHash(), FieldOriginBuilder.configHash(reversed));
            reversed.get(0).setSourceField("other");
            assertNotEquals(result.getConfigHash(), FieldOriginBuilder.configHash(reversed));
        }

        @Test
        void taskWithoutTargetHasNoRecords() {
            source("s", "Player", "id");
            FieldOriginBuildResult result = build();
            assertTrue(result.getRecords().isEmpty());
            assertEquals(0, result.getStats().getTargetTables());
        }
    }
}
