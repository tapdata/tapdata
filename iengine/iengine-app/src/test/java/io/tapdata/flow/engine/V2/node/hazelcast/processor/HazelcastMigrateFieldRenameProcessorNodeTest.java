package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTaskTest;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import io.tapdata.MockTaskUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-07-24 16:28
 **/
class HazelcastMigrateFieldRenameProcessorNodeTest extends BaseTaskTest {
    private static final String TAG = HazelcastMigrateFieldRenameProcessorNodeTest.class.getSimpleName();
    private MigrateFieldRenameProcessorNode migrateFieldRenameProcessorNode;
    private HazelcastMigrateFieldRenameProcessorNode hazelcastMigrateFieldRenameProcessorNode;

    @BeforeEach
    void beforeEach() {
        taskDto = MockTaskUtil.setUpTaskDtoByJsonFile(String.join(File.separator, TAG, "tryProcessTest1.json"));
        migrateFieldRenameProcessorNode = (MigrateFieldRenameProcessorNode) taskDto.getDag().getNodes().stream().filter(n -> n instanceof MigrateFieldRenameProcessorNode).findFirst().orElse(null);
        setupContext(migrateFieldRenameProcessorNode);
        hazelcastMigrateFieldRenameProcessorNode = new HazelcastMigrateFieldRenameProcessorNode(processorBaseContext);
    }

    @Nested
    class replaceValueIfNeedTest {
        String oldKey;
        String newKey;
        Map<String, Object> originValueMap;
        Map<String, Object> param;
        @BeforeEach
        void beforeEach() {
            oldKey = "A";
            newKey = "B";
            originValueMap = new HashMap<>();
            param = new HashMap<>();
            param.put("A", 1);
            param.put("B", 2);
        }
        @Test
        @DisplayName("test when originValueMap not contains old key")
        void test1() {
            hazelcastMigrateFieldRenameProcessorNode.replaceValueIfNeed(oldKey, newKey, originValueMap, param);
            assertNull(param.get("A"));
        }
        @Test
        @DisplayName("test when originValueMap contains old key")
        void test2() {
            hazelcastMigrateFieldRenameProcessorNode.replaceValueIfNeed("B", newKey, originValueMap, param);
            assertEquals(1, param.get("A"));
            assertEquals(2, param.get("B"));
        }
    }
}