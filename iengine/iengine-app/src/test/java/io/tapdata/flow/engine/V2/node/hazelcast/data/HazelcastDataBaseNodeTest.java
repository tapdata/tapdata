package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.hazelcast.jet.core.JobStatus;
import com.tapdata.constant.StringCompression;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.monitor.impl.JetJobStatusMonitor;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastDataBaseNode.STREAM_OFFSET_COMPRESS_PREFIX;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/18 16:45
 */
public class HazelcastDataBaseNodeTest {

    private HazelcastDataBaseNode dataBaseNode;

    @BeforeEach
    void beforeEach() {
        DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);

        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());

        when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);

        dataBaseNode = new HazelcastDataBaseNode(dataProcessorContext) {

        };
    }

    @Test
    void need2InitialSync() {
        SyncProgress syncProgress = new SyncProgress();
        dataBaseNode.running.set(false);
        Assertions.assertFalse(dataBaseNode.need2InitialSync(syncProgress));

        dataBaseNode.running.set(true);
        ReflectionTestUtils.setField(dataBaseNode, "syncType", SyncTypeEnum.CDC);
        Assertions.assertFalse(dataBaseNode.need2InitialSync(syncProgress));

        ReflectionTestUtils.setField(dataBaseNode, "syncType", SyncTypeEnum.INITIAL_SYNC_CDC);
        JetJobStatusMonitor jetJobStatusMonitor = mock(JetJobStatusMonitor.class);
        when(jetJobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);
        ReflectionTestUtils.setField(dataBaseNode, "jetJobStatusMonitor", jetJobStatusMonitor);
        Assertions.assertTrue(dataBaseNode.need2InitialSync(syncProgress));

        Map<String, Object> map = new HashMap<>();
        Map<String, Object> offset = new HashMap<>();
        offset.put("batch_read_connector_status", "over");
        map.put("test", offset);
        syncProgress.setBatchOffsetObj(map);

        Assertions.assertTrue(dataBaseNode.need2InitialSync(syncProgress));

        syncProgress.setBatchOffsetObj(null);
        syncProgress.setSyncStage(SyncStage.CDC.name());
        Assertions.assertFalse(dataBaseNode.need2InitialSync(syncProgress));
    }

    @Test
    void testUncompressStreamOffsetIfNeed() throws IOException {
        String str = StringCompression.compress("test");
        String result = dataBaseNode.uncompressStreamOffsetIfNeed(STREAM_OFFSET_COMPRESS_PREFIX + str);

        Assertions.assertEquals("test", result);

    }

}
