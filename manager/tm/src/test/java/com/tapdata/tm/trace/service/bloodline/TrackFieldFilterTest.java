package com.tapdata.tm.trace.service.bloodline;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.lineage.analyzer.entity.LineageMetadataInstance;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTaskNode;
import com.tapdata.tm.commons.task.dto.Dag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TrackFieldFilterTest {
    private FieldOriginalNameMapping fieldOriginalNameMapping;
    private TrackFieldFilter filter;

    @BeforeEach
    void setUp() {
        fieldOriginalNameMapping = mock(FieldOriginalNameMapping.class);
        filter = new TrackFieldFilter();
        filter.fieldOriginalNameMapping = fieldOriginalNameMapping;
    }

    @Test
    void removeUselessFields_shouldReturnEmpty_whenDagOrNodesNull() {
        assertEquals(Map.of(), filter.removeUselessFields(null, List.of("a"), Map.of()));

        Dag dag = new Dag();
        dag.setNodes(null);
        assertEquals(Map.of(), filter.removeUselessFields(dag, List.of("a"), Map.of()));
    }

    @Test
    void removeUselessFields_shouldFilterDagAndClearMetadataFields() {
        LineageMetadataInstance metaTarget = new LineageMetadataInstance();
        metaTarget.setFields(List.of(new com.tapdata.tm.commons.schema.Field()));
        LineageTableNode target = new LineageTableNode("t", "c", "cn", "pdk", metaTarget);
        target.setId("target");

        LineageMetadataInstance metaUp = new LineageMetadataInstance();
        metaUp.setFields(List.of(new com.tapdata.tm.commons.schema.Field()));
        LineageTableNode upstream = new LineageTableNode("t2", "c2", "cn2", "pdk2", metaUp);
        upstream.setId("up");

        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>(List.of(upstream, target)));
        dag.setEdges(new ArrayList<>(List.of(new Edge("up", "target"))));

        when(fieldOriginalNameMapping.findFinalTargetLineageTableNode(eq(dag))).thenReturn(target);

        Map<String, Map<String, String>> fieldNameMapping = new HashMap<>();
        fieldNameMapping.put("target", Map.of("tField", "id", "id", "id"));
        fieldNameMapping.put("up", Map.of("id", "id", "a", "id"));

        Map<String, Map<String, String>> result = filter.removeUselessFields(dag, List.of("tField"), fieldNameMapping);

        assertEquals(Map.of("target", Map.of("tField", "tField"), "up", Map.of("id", "id")), result);
        assertEquals(2, dag.getNodes().size());
        assertEquals(1, dag.getEdges().size());
        assertNull(metaTarget.getFields());
        assertNull(metaUp.getFields());
    }

    @Test
    void removeUnTraceFilterFieldName_shouldReturnEmpty_whenTraceFilterEmpty() {
        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>());
        assertEquals(Map.of(), filter.removeUnTraceFilterFieldName(dag, null, Map.of()));
        assertEquals(Map.of(), filter.removeUnTraceFilterFieldName(dag, new ArrayList<>(), Map.of()));
    }

    @Test
    void removeUnTraceFilterFieldName_shouldReturnEmpty_whenDagInvalidOrNoFinalTarget() {
        assertEquals(Map.of(), filter.removeUnTraceFilterFieldName(null, List.of("a"), Map.of()));

        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>());
        assertEquals(Map.of(), filter.removeUnTraceFilterFieldName(dag, List.of("a"), Map.of()));

        LineageTableNode target = new LineageTableNode("t", "c", "cn", "pdk", new LineageMetadataInstance());
        target.setId(" ");
        dag.setNodes(List.of(target));
        when(fieldOriginalNameMapping.findFinalTargetLineageTableNode(eq(dag))).thenReturn(target);
        assertEquals(Map.of(), filter.removeUnTraceFilterFieldName(dag, List.of("a"), Map.of()));
    }

    @Test
    void removeUnTraceFilterFieldName_shouldClearDag_whenNoIntersectionWithTargetFieldKeys() {
        LineageTableNode target = new LineageTableNode("t", "c", "cn", "pdk", new LineageMetadataInstance());
        target.setId("target");
        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>(List.of(target)));
        dag.setEdges(new ArrayList<>(List.of(new Edge("x", "target"))));

        when(fieldOriginalNameMapping.findFinalTargetLineageTableNode(eq(dag))).thenReturn(target);

        Map<String, Map<String, String>> fieldNameMapping = Map.of("target", Map.of("tField", "id"));

        Map<String, Map<String, String>> result = filter.removeUnTraceFilterFieldName(dag, List.of("notExist"), fieldNameMapping);

        assertEquals(Map.of(), result);
        assertNotNull(dag.getNodes());
        assertNotNull(dag.getEdges());
        assertEquals(0, dag.getNodes().size());
        assertEquals(0, dag.getEdges().size());
    }

    @Test
    void merge_shouldHandleNullAndIntersection() {
        assertEquals(Set.of(), filter.merge(null, Set.of("a")));
        assertEquals(Set.of(), filter.merge(Set.of("a"), null));
        assertEquals(Set.of("b"), filter.merge(Set.of("a", "b"), Set.of("b", "c")));
    }

    @Test
    void upstreamNodeIds_shouldReturnAllMatchingSources() {
        List<Edge> edges = List.of(
                new Edge("a", "t"),
                new Edge("b", "t"),
                new Edge("x", "y")
        );
        assertEquals(Set.of("a", "b"), filter.upstreamNodeIds("t", edges));
        assertEquals(Set.of(), filter.upstreamNodeIds("none", edges));
    }

    @Test
    void eachAllNodes_shouldCoverNonTableAndTargetAndNonTargetBranches() {
        Set<String> kept = new HashSet<>();
        Map<String, Map<String, String>> fieldNameMappingByNodeId = new HashMap<>();
        Map<String, Map<String, String>> result = new HashMap<>();

        LineageTaskNode nonTable = new LineageTaskNode("x", "n", "t");
        filter.eachAllNodes(nonTable, "target", Set.of("f"), kept, fieldNameMappingByNodeId, result, Map.of("f", "o"));
        assertEquals(0, kept.size());
        assertEquals(0, result.size());

        LineageTableNode target = new LineageTableNode("t", "c", "cn", "pdk", new LineageMetadataInstance());
        target.setId("target");
        filter.eachAllNodes(target, "target", Set.of("tField"), kept, fieldNameMappingByNodeId, result, Map.of());
        assertEquals(Map.of("target", Map.of("tField", "tField")), result);

        LineageTableNode up = new LineageTableNode("t", "c", "cn", "pdk", new LineageMetadataInstance());
        up.setId("up");
        fieldNameMappingByNodeId.put("up", Map.of("a", "id", "b", "id", "id", "id"));

        filter.eachAllNodes(up, "target", Set.of("id"), kept, fieldNameMappingByNodeId, result, Map.of("id", "id"));
        assertTrue(kept.contains("up"));
        assertEquals(Map.of("id", "id"), result.get("up"));

        LineageTableNode noMapping = new LineageTableNode("t", "c", "cn", "pdk", new LineageMetadataInstance());
        noMapping.setId("noMapping");
        filter.eachAllNodes(noMapping, "target", Set.of("id"), kept, fieldNameMappingByNodeId, result, Map.of("id", "id"));
        assertNull(result.get("noMapping"));
    }

    @Test
    void eachTargetFieldToOriginName_andEachNodeFieldToOriginToFindBestName_shouldCoverBranches() {
        Map<String, String> nodeFieldToOrigin = Map.of("a", "id", "b", "id", "id", "id", " ", "id");
        Map<String, String> mapping = new HashMap<>();

        filter.eachTargetFieldToOriginName(Map.entry("t", " "), nodeFieldToOrigin, mapping);
        assertEquals(0, mapping.size());

        filter.eachTargetFieldToOriginName(Map.entry("id", "id"), nodeFieldToOrigin, mapping);
        assertEquals("id", mapping.get("id"));

        mapping.clear();
        filter.eachTargetFieldToOriginName(Map.entry("z", "id"), nodeFieldToOrigin, mapping);
        assertEquals("a", mapping.get("z"));

        assertNull(filter.eachNodeFieldToOriginToFindBestName("nope", nodeFieldToOrigin, "z"));
    }
}

