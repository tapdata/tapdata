package io.tapdata.flow.engine.V2.node.hazelcast.controller;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.exception.TapCodeException;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tapdata.tm.commons.dag.process.MergeTableNode.MAIN_TABLE_FIRST_MERGE_MODE;
import static com.tapdata.tm.commons.dag.process.MergeTableNode.SUB_TABLE_FIRST_MERGE_MODE;
import static io.tapdata.flow.engine.V2.node.hazelcast.controller.SnapshotOrderService.SNAPSHOT_ORDER_LIST_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotOrderServiceTest {
    SnapshotOrderService snapshotOrderService;
    @BeforeEach
    void setup() {
        snapshotOrderService = new SnapshotOrderService();
    }
    @Test
    void addControllerTest_SNAPSHOT_ORDER_LIST_FORMAT_ERROR() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(mock(ObjectId.class));
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(SNAPSHOT_ORDER_LIST_KEY, new HashMap<>());
        taskDto.setAttrs(attrs);
        TapCodeException exception = assertThrows(TapCodeException.class, () -> snapshotOrderService.addController(taskDto));
        assertEquals(SnapshotOrderControllerExCode_21.SNAPSHOT_ORDER_LIST_FORMAT_ERROR, exception.getCode());

    }
    @Test
    void handleSnapshotOrderMode_CANNOT_CHANGE_MERGE_MODE_WITH_OUT_RESET() {
        TaskDto taskDto = mock(TaskDto.class);
        DAG dag = mock(DAG.class);
        when(taskDto.getDag()).thenReturn(dag);
        java.util.List<Node> nodes = new ArrayList<>();
        MergeTableNode mergeTableNode = mock(MergeTableNode.class);
        when(mergeTableNode.getMergeMode()).thenReturn(MAIN_TABLE_FIRST_MERGE_MODE);
        nodes.add(mergeTableNode);
        when(dag.getNodes()).thenReturn(nodes);
        String oldMergeMode = SUB_TABLE_FIRST_MERGE_MODE;
        List<NodeControlLayer> snapshotOrderList = new ArrayList();
        TapCodeException exception = assertThrows(TapCodeException.class, () -> snapshotOrderService.handleSnapshotOrderMode(taskDto, oldMergeMode, snapshotOrderList));
        assertEquals(SnapshotOrderControllerExCode_21.CANNOT_CHANGE_MERGE_MODE_WITH_OUT_RESET, exception.getCode());
    }

}
